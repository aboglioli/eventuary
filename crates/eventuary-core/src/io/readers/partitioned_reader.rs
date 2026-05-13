//! Partitioned reader: tags each delivered message with a logical
//! partition id derived from `partition_for(event, count)`. Passes the
//! inner acker through unchanged, so source-level redelivery semantics
//! still apply.

use std::num::{NonZeroU16, NonZeroUsize};
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{Stream, StreamExt};

use crate::error::Result;
use crate::io::{Acker, Message, Reader};
use crate::partition::{CursorPartition, LogicalPartition, partition_for};

#[derive(Debug, Clone)]
pub struct PartitionedReaderConfig {
    pub partition_count: NonZeroU16,
    pub lane_capacity: NonZeroUsize,
    pub scheduling: LaneScheduling,
}

impl Default for PartitionedReaderConfig {
    fn default() -> Self {
        Self {
            partition_count: NonZeroU16::new(64).unwrap(),
            lane_capacity: NonZeroUsize::new(128).unwrap(),
            scheduling: LaneScheduling::RoundRobin,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum LaneScheduling {
    RoundRobin,
    QueueDepthWeighted { max_burst_per_lane: NonZeroUsize },
}

#[derive(Debug, Clone)]
pub struct PartitionedSubscription<S> {
    pub inner: S,
}

impl<S> PartitionedSubscription<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PartitionedCursor<C> {
    inner: C,
    partition: LogicalPartition,
}

impl<C> PartitionedCursor<C> {
    pub fn new(inner: C, partition: LogicalPartition) -> Self {
        Self { inner, partition }
    }

    pub fn inner(&self) -> &C {
        &self.inner
    }

    pub fn partition(&self) -> LogicalPartition {
        self.partition
    }
}

impl<C> CursorPartition for PartitionedCursor<C> {
    fn partition(&self) -> Option<LogicalPartition> {
        Some(self.partition)
    }
}

pub struct PartitionAcker<A: Acker>(pub A);

impl<A: Acker> Acker for PartitionAcker<A> {
    fn ack(&self) -> impl std::future::Future<Output = Result<()>> + Send {
        self.0.ack()
    }
    fn nack(&self) -> impl std::future::Future<Output = Result<()>> + Send {
        self.0.nack()
    }
}

pub type PartitionedStream<A, C> =
    Pin<Box<dyn Stream<Item = Result<Message<PartitionAcker<A>, PartitionedCursor<C>>>> + Send>>;

pub struct PartitionedReader<R> {
    inner: R,
    config: PartitionedReaderConfig,
}

impl<R> PartitionedReader<R> {
    pub fn new(inner: R, config: PartitionedReaderConfig) -> Self {
        Self { inner, config }
    }
}

impl<R> Reader for PartitionedReader<R>
where
    R: Reader + Send + Sync + 'static,
    R::Subscription: Send + 'static,
    R::Stream: Send + 'static,
    R::Acker: Acker + Send + Sync + 'static,
    R::Cursor: Send + Sync + 'static,
{
    type Subscription = PartitionedSubscription<R::Subscription>;
    type Acker = PartitionAcker<R::Acker>;
    type Cursor = PartitionedCursor<R::Cursor>;
    type Stream = PartitionedStream<R::Acker, R::Cursor>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let inner = self.inner.read(subscription.inner).await?;
        let count_u16 = self.config.partition_count;
        let count_u32 = std::num::NonZeroU32::new(count_u16.get() as u32).unwrap();
        let mapped = inner.map(move |item| match item {
            Ok(msg) => {
                let lane = partition_for(msg.event(), count_u32) as u16;
                let partition = LogicalPartition::new(lane, count_u16).expect("valid lane");
                let (event, acker, cursor) = msg.into_parts();
                Ok(Message::new(
                    event,
                    PartitionAcker(acker),
                    PartitionedCursor::new(cursor, partition),
                ))
            }
            Err(e) => Err(e),
        });
        Ok(Box::pin(mapped))
    }
}

fn _poll_dummy<S: Stream>(_: Pin<&mut S>, _: &mut Context<'_>) -> Poll<()> {
    Poll::Ready(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Event;
    use crate::io::Message;
    use crate::io::acker::NoopAcker;
    use crate::payload::Payload;

    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    struct TestCursor(i64);

    struct VecReader {
        events: std::sync::Mutex<Option<Vec<Event>>>,
    }

    impl Reader for VecReader {
        type Subscription = ();
        type Acker = NoopAcker;
        type Cursor = TestCursor;
        type Stream = Pin<Box<dyn Stream<Item = Result<Message<NoopAcker, TestCursor>>> + Send>>;

        async fn read(&self, _: ()) -> Result<Self::Stream> {
            let events = self.events.lock().unwrap().take().unwrap();
            let iter = events
                .into_iter()
                .enumerate()
                .map(|(i, e)| Ok(Message::new(e, NoopAcker, TestCursor(i as i64 + 1))));
            Ok(Box::pin(futures::stream::iter(iter)))
        }
    }

    fn ev(key: &str) -> Event {
        Event::builder("acme", "/x", "thing.happened", Payload::from_string("p"))
            .unwrap()
            .key(key)
            .unwrap()
            .build()
            .expect("valid event")
    }

    #[tokio::test]
    async fn tags_cursor_with_partition() {
        let events = (0..16).map(|i| ev(&format!("k{i}"))).collect::<Vec<_>>();
        let reader = VecReader {
            events: std::sync::Mutex::new(Some(events)),
        };
        let p = PartitionedReader::new(
            reader,
            PartitionedReaderConfig {
                partition_count: NonZeroU16::new(4).unwrap(),
                ..PartitionedReaderConfig::default()
            },
        );
        let mut stream = p.read(PartitionedSubscription::new(())).await.unwrap();
        let mut delivered = 0usize;
        while let Some(item) = stream.next().await {
            let msg = item.unwrap();
            assert!(msg.cursor().partition().id() < 4);
            msg.ack().await.unwrap();
            delivered += 1;
        }
        assert_eq!(delivered, 16);
    }
}
