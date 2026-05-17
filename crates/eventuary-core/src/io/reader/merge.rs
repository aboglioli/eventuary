use std::pin::Pin;

use futures::stream::{PollNext, select_with_strategy};
use futures::{Stream, StreamExt};

use crate::error::Result;
use crate::io::{Acker, Cursor, CursorId, Message, Reader};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub enum MergeStrategy {
    #[default]
    Fair,
    LeftPriority,
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
        };
        Ok(merged)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum MergeCursor<C1, C2> {
    Left(C1),
    Right(C2),
}

impl<C1: Cursor, C2: Cursor> Cursor for MergeCursor<C1, C2> {
    fn id(&self) -> CursorId {
        match self {
            Self::Left(c) => c.id(),
            Self::Right(c) => c.id(),
        }
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

    #[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
    struct TestCursor(u64);

    impl Cursor for TestCursor {}

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
    async fn merge_cursor_id_delegates_to_inner() {
        let left: MergeCursor<TestCursor, TestCursor> = MergeCursor::Left(TestCursor(1));
        let right: MergeCursor<TestCursor, TestCursor> = MergeCursor::Right(TestCursor(2));
        assert_eq!(left.id(), TestCursor(1).id());
        assert_eq!(right.id(), TestCursor(2).id());
    }
}
