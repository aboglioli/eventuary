use std::num::NonZeroUsize;
use std::pin::Pin;

use futures::stream::{PollNext, select_with_strategy};
use futures::{Stream, StreamExt};

use crate::error::Result;
use crate::io::acker::NackContext;
use crate::io::position::{StartFrom, StartableSubscription};
use crate::io::{Acker, Cursor, CursorId, CursorOrder, Message, Reader};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub enum MergeStrategy {
    #[default]
    Fair,
    LeftPriority,
    Weighted {
        left: NonZeroUsize,
        right: NonZeroUsize,
    },
}

pub struct MergeReader<R1, R2> {
    r1: R1,
    r2: R2,
    strategy: MergeStrategy,
}

impl<R1, R2> MergeReader<R1, R2> {
    pub fn new(r1: R1, r2: R2) -> Self {
        Self {
            r1,
            r2,
            strategy: MergeStrategy::Fair,
        }
    }

    pub fn with_left_priority(r1: R1, r2: R2) -> Self {
        Self {
            r1,
            r2,
            strategy: MergeStrategy::LeftPriority,
        }
    }

    pub fn with_weighted(r1: R1, r2: R2, left: NonZeroUsize, right: NonZeroUsize) -> Self {
        Self {
            r1,
            r2,
            strategy: MergeStrategy::Weighted { left, right },
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct WeightedState {
    left: NonZeroUsize,
    right: NonZeroUsize,
    remaining_left: usize,
    remaining_right: usize,
}

impl WeightedState {
    fn new(left: NonZeroUsize, right: NonZeroUsize) -> Self {
        Self {
            left,
            right,
            remaining_left: left.get(),
            remaining_right: right.get(),
        }
    }

    fn next(&mut self) -> PollNext {
        if self.remaining_left == 0 && self.remaining_right == 0 {
            self.remaining_left = self.left.get();
            self.remaining_right = self.right.get();
        }
        if self.remaining_left > 0 {
            self.remaining_left -= 1;
            return PollNext::Left;
        }
        self.remaining_right -= 1;
        PollNext::Right
    }
}

impl<R1, R2> Reader for MergeReader<R1, R2>
where
    R1: Reader + Send + Sync + 'static,
    R2: Reader + Send + Sync + 'static,
    R1::Subscription: Send + 'static,
    R2::Subscription: Send + 'static,
    R1::Acker: Send + Sync + 'static,
    R2::Acker: Send + Sync + 'static,
    R1::Cursor: Cursor + Send + Sync + 'static,
    R2::Cursor: Cursor + Send + Sync + 'static,
    R1::Stream: Send + 'static,
    R2::Stream: Send + 'static,
{
    type Subscription = (R1::Subscription, R2::Subscription);
    type Acker = MergeAcker<R1::Acker, R2::Acker>;
    type Cursor = MergeCursor<R1::Cursor, R2::Cursor>;
    type Stream = Pin<Box<dyn Stream<Item = Result<Message<Self::Acker, Self::Cursor>>> + Send>>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let (sub1, sub2) = subscription;
        let stream1 = self.r1.read(sub1).await?;
        let stream2 = self.r2.read(sub2).await?;

        let s1 = stream1.map(|item| {
            item.map(|msg| {
                let (event, acker, cursor) = msg.into_parts();
                Message::new(event, MergeAcker::Left(acker), MergeCursor::Left(cursor))
            })
        });
        let s2 = stream2.map(|item| {
            item.map(|msg| {
                let (event, acker, cursor) = msg.into_parts();
                Message::new(event, MergeAcker::Right(acker), MergeCursor::Right(cursor))
            })
        });

        let merged: Self::Stream = match self.strategy {
            MergeStrategy::Fair => Box::pin(futures::stream::select(s1, s2)),
            MergeStrategy::LeftPriority => {
                Box::pin(select_with_strategy(s1, s2, |_: &mut ()| PollNext::Left))
            }
            MergeStrategy::Weighted { left, right } => {
                let mut state = WeightedState::new(left, right);
                Box::pin(select_with_strategy(s1, s2, move |_: &mut ()| state.next()))
            }
        };
        Ok(merged)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum MergeCursor<C1, C2> {
    Left(C1),
    Right(C2),
}

impl<C1: Cursor, C2: Cursor> Cursor for MergeCursor<C1, C2> {
    fn id(&self) -> CursorId {
        match self {
            Self::Left(c) => c.id().prefixed("left").expect("valid left cursor id"),
            Self::Right(c) => c.id().prefixed("right").expect("valid right cursor id"),
        }
    }

    fn order_key(&self) -> CursorOrder {
        match self {
            Self::Left(c) => c.order_key(),
            Self::Right(c) => c.order_key(),
        }
    }
}

impl<S1, S2, C1, C2> StartableSubscription<MergeCursor<C1, C2>> for (S1, S2)
where
    S1: StartableSubscription<C1> + Clone + Send + 'static,
    S2: StartableSubscription<C2> + Clone + Send + 'static,
    C1: Ord + Send + 'static,
    C2: Ord + Send + 'static,
{
    fn with_start(self, start: StartFrom<MergeCursor<C1, C2>>) -> Self {
        match start {
            StartFrom::Earliest => (
                self.0.with_start(StartFrom::Earliest),
                self.1.with_start(StartFrom::Earliest),
            ),
            StartFrom::Latest => (
                self.0.with_start(StartFrom::Latest),
                self.1.with_start(StartFrom::Latest),
            ),
            StartFrom::Timestamp(timestamp) => (
                self.0.with_start(StartFrom::Timestamp(timestamp)),
                self.1.with_start(StartFrom::Timestamp(timestamp)),
            ),
            StartFrom::After(MergeCursor::Left(cursor)) => {
                (self.0.with_start(StartFrom::After(cursor)), self.1)
            }
            StartFrom::After(MergeCursor::Right(cursor)) => {
                (self.0, self.1.with_start(StartFrom::After(cursor)))
            }
        }
    }

    fn with_starts(self, starts: Vec<StartFrom<MergeCursor<C1, C2>>>) -> Self {
        let mut left_starts = Vec::new();
        let mut right_starts = Vec::new();

        for start in starts {
            match start {
                StartFrom::Earliest => {
                    left_starts.push(StartFrom::Earliest);
                    right_starts.push(StartFrom::Earliest);
                }
                StartFrom::Latest => {
                    left_starts.push(StartFrom::Latest);
                    right_starts.push(StartFrom::Latest);
                }
                StartFrom::Timestamp(timestamp) => {
                    left_starts.push(StartFrom::Timestamp(timestamp));
                    right_starts.push(StartFrom::Timestamp(timestamp));
                }
                StartFrom::After(MergeCursor::Left(cursor)) => {
                    left_starts.push(StartFrom::After(cursor));
                }
                StartFrom::After(MergeCursor::Right(cursor)) => {
                    right_starts.push(StartFrom::After(cursor));
                }
            }
        }

        let left = if left_starts.is_empty() {
            self.0
        } else {
            self.0.with_starts(left_starts)
        };
        let right = if right_starts.is_empty() {
            self.1
        } else {
            self.1.with_starts(right_starts)
        };

        (left, right)
    }
}

pub enum MergeAcker<A1, A2> {
    Left(A1),
    Right(A2),
}

impl<A1: Acker, A2: Acker> Acker for MergeAcker<A1, A2> {
    async fn ack(&self) -> Result<()> {
        match self {
            Self::Left(a) => a.ack().await,
            Self::Right(a) => a.ack().await,
        }
    }

    async fn nack(&self) -> Result<()> {
        match self {
            Self::Left(a) => a.nack().await,
            Self::Right(a) => a.nack().await,
        }
    }

    async fn nack_with(&self, context: NackContext) -> Result<()> {
        match self {
            Self::Left(a) => a.nack_with(context).await,
            Self::Right(a) => a.nack_with(context).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::time::Duration;

    use futures::{Stream, StreamExt, stream};

    use super::*;
    use crate::event::Event;
    use crate::io::acker::NoopAcker;
    use crate::payload::Payload;

    #[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
    struct TestCursor(u64);

    impl Cursor for TestCursor {
        fn order_key(&self) -> CursorOrder {
            CursorOrder::from_u64(self.0)
        }
    }

    struct VecReader {
        events: Mutex<Option<Vec<Event>>>,
    }

    impl Reader for VecReader {
        type Subscription = ();
        type Acker = NoopAcker;
        type Cursor = TestCursor;
        type Stream = Pin<Box<dyn Stream<Item = Result<Message<NoopAcker, TestCursor>>> + Send>>;

        async fn read(&self, _: ()) -> Result<Self::Stream> {
            let events = self.events.lock().unwrap().take().unwrap_or_default();
            Ok(Box::pin(stream::iter(events.into_iter().enumerate().map(
                |(i, e)| Ok(Message::new(e, NoopAcker, TestCursor(i as u64))),
            ))))
        }
    }

    fn ev(topic: &str) -> Event {
        Event::create("org", "/x", topic, Payload::from_string("p")).unwrap()
    }

    #[tokio::test]
    async fn merges_two_readers() {
        let reader1 = VecReader {
            events: Mutex::new(Some(vec![ev("a"), ev("a"), ev("a")])),
        };
        let reader2 = VecReader {
            events: Mutex::new(Some(vec![ev("b"), ev("b")])),
        };
        let merged = MergeReader::new(reader1, reader2);
        let mut stream = merged.read(((), ())).await.unwrap();

        let mut count = 0usize;
        for _ in 0..5 {
            let msg = tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            msg.ack().await.unwrap();
            count += 1;
        }
        assert_eq!(count, 5);
        let end = tokio::time::timeout(Duration::from_millis(200), stream.next()).await;
        assert!(end.is_err() || end.unwrap().is_none());
    }

    #[tokio::test]
    async fn left_priority_drains_left_before_right() {
        let left = VecReader {
            events: Mutex::new(Some(vec![ev("a"), ev("a"), ev("a")])),
        };
        let right = VecReader {
            events: Mutex::new(Some(vec![ev("b"), ev("b")])),
        };
        let merged = MergeReader::with_left_priority(left, right);
        let mut stream = merged.read(((), ())).await.unwrap();
        let mut topics: Vec<String> = Vec::new();
        for _ in 0..5 {
            let msg = tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            topics.push(msg.event().topic().as_str().to_owned());
            msg.ack().await.unwrap();
        }
        assert_eq!(topics, vec!["a", "a", "a", "b", "b"]);
    }

    #[tokio::test]
    async fn merge_cursor_ids_are_prefixed() {
        let left: MergeCursor<TestCursor, TestCursor> = MergeCursor::Left(TestCursor(1));
        let right: MergeCursor<TestCursor, TestCursor> = MergeCursor::Right(TestCursor(2));
        assert_eq!(left.id().as_str(), "left:global");
        assert_eq!(right.id().as_str(), "right:global");
    }

    #[tokio::test]
    async fn merge_cursor_order_passes_through_active_side() {
        let l: MergeCursor<TestCursor, TestCursor> = MergeCursor::Left(TestCursor(7));
        assert_eq!(l.order_key(), TestCursor(7).order_key());
    }

    #[derive(Debug, Clone, Default, Eq, PartialEq)]
    struct TestSub {
        starts: Vec<StartFrom<TestCursor>>,
    }

    impl StartableSubscription<TestCursor> for TestSub {
        fn with_start(mut self, start: StartFrom<TestCursor>) -> Self {
            self.starts = vec![start];
            self
        }

        fn with_starts(mut self, starts: Vec<StartFrom<TestCursor>>) -> Self {
            self.starts = starts;
            self
        }
    }

    #[tokio::test]
    async fn weighted_strategy_prefers_left_by_weight() {
        let reader1 = VecReader {
            events: Mutex::new(Some(vec![
                ev("left.1"),
                ev("left.2"),
                ev("left.3"),
                ev("left.4"),
            ])),
        };
        let reader2 = VecReader {
            events: Mutex::new(Some(vec![ev("right.1"), ev("right.2")])),
        };
        let merged = MergeReader::with_weighted(
            reader1,
            reader2,
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        );
        let mut stream = merged.read(((), ())).await.unwrap();

        let mut topics = Vec::new();
        for _ in 0..6 {
            let msg = tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            topics.push(msg.event().topic().as_str().to_owned());
        }

        assert_eq!(
            topics,
            vec!["left.1", "left.2", "right.1", "left.3", "left.4", "right.2",]
        );
    }

    #[tokio::test]
    async fn weighted_strategy_equal_weights_alternates() {
        let left = VecReader {
            events: Mutex::new(Some(vec![ev("l1"), ev("l2"), ev("l3")])),
        };
        let right = VecReader {
            events: Mutex::new(Some(vec![ev("r1"), ev("r2"), ev("r3")])),
        };
        let merged = MergeReader::with_weighted(
            left,
            right,
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        );
        let mut stream = merged.read(((), ())).await.unwrap();

        let mut topics = Vec::new();
        for _ in 0..6 {
            let msg = tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            topics.push(msg.event().topic().as_str().to_owned());
        }

        assert_eq!(topics, vec!["l1", "r1", "l2", "r2", "l3", "r3"]);
    }

    #[tokio::test]
    async fn weighted_strategy_drains_left_when_right_is_empty() {
        let left = VecReader {
            events: Mutex::new(Some(vec![ev("l1"), ev("l2"), ev("l3")])),
        };
        let right = VecReader {
            events: Mutex::new(Some(vec![])),
        };
        let merged = MergeReader::with_weighted(
            left,
            right,
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(5).unwrap(),
        );
        let mut stream = merged.read(((), ())).await.unwrap();

        let mut topics = Vec::new();
        for _ in 0..3 {
            let msg = tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            topics.push(msg.event().topic().as_str().to_owned());
        }

        assert_eq!(topics, vec!["l1", "l2", "l3"]);
    }

    #[test]
    fn merge_strategy_default_is_fair() {
        assert_eq!(MergeStrategy::default(), MergeStrategy::Fair);
    }

    #[test]
    fn weighted_strategy_equality_matches_components() {
        let a = MergeStrategy::Weighted {
            left: NonZeroUsize::new(3).unwrap(),
            right: NonZeroUsize::new(1).unwrap(),
        };
        let b = MergeStrategy::Weighted {
            left: NonZeroUsize::new(3).unwrap(),
            right: NonZeroUsize::new(1).unwrap(),
        };
        let c = MergeStrategy::Weighted {
            left: NonZeroUsize::new(2).unwrap(),
            right: NonZeroUsize::new(1).unwrap(),
        };
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    struct ContextAcker {
        side: &'static str,
        captured: std::sync::Arc<Mutex<Vec<(&'static str, NackContext)>>>,
    }

    impl Acker for ContextAcker {
        async fn ack(&self) -> Result<()> {
            Ok(())
        }
        async fn nack(&self) -> Result<()> {
            Ok(())
        }
        async fn nack_with(&self, context: NackContext) -> Result<()> {
            self.captured.lock().unwrap().push((self.side, context));
            Ok(())
        }
    }

    #[tokio::test]
    async fn merge_forwards_nack_context_to_selected_side() {
        let captured = std::sync::Arc::new(Mutex::new(Vec::new()));
        let left: MergeAcker<ContextAcker, ContextAcker> = MergeAcker::Left(ContextAcker {
            side: "left",
            captured: std::sync::Arc::clone(&captured),
        });
        left.nack_with(NackContext::processing_rejected("L").unwrap())
            .await
            .unwrap();

        let right: MergeAcker<ContextAcker, ContextAcker> = MergeAcker::Right(ContextAcker {
            side: "right",
            captured: std::sync::Arc::clone(&captured),
        });
        right
            .nack_with(NackContext::processing_rejected("R").unwrap())
            .await
            .unwrap();

        let captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 2);
        assert_eq!(captured[0].0, "left");
        assert_eq!(captured[0].1.context().message(), "L");
        assert_eq!(captured[1].0, "right");
        assert_eq!(captured[1].1.context().message(), "R");
    }

    #[test]
    fn merge_subscription_routes_starts_by_branch() {
        let subscription = (TestSub::default(), TestSub::default()).with_starts(vec![
            StartFrom::After(MergeCursor::Left(TestCursor(3))),
            StartFrom::After(MergeCursor::Right(TestCursor(8))),
            StartFrom::After(MergeCursor::Left(TestCursor(5))),
        ]);

        assert_eq!(
            subscription.0.starts,
            vec![
                StartFrom::After(TestCursor(3)),
                StartFrom::After(TestCursor(5))
            ]
        );
        assert_eq!(subscription.1.starts, vec![StartFrom::After(TestCursor(8))]);
    }
}
