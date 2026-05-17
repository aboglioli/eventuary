use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::{Stream, StreamExt};

use crate::error::Result;
use crate::io::message::Message;
use crate::io::{Acker, BoxAcker};

pub type BoxStream<C, A = BoxAcker> = Pin<Box<dyn Stream<Item = Result<Message<A, C>>> + Send>>;

pub trait Reader: Send + Sync {
    type Subscription: Send;
    type Acker: Acker;
    type Cursor: Send;
    type Stream: Stream<Item = Result<Message<Self::Acker, Self::Cursor>>> + Send;

    fn read(
        &self,
        subscription: Self::Subscription,
    ) -> impl Future<Output = Result<Self::Stream>> + Send;
}

impl<T: Reader + ?Sized> Reader for Arc<T> {
    type Subscription = T::Subscription;
    type Acker = T::Acker;
    type Cursor = T::Cursor;
    type Stream = T::Stream;

    fn read(
        &self,
        subscription: Self::Subscription,
    ) -> impl Future<Output = Result<Self::Stream>> + Send {
        (**self).read(subscription)
    }
}

impl<T: Reader + ?Sized> Reader for Box<T> {
    type Subscription = T::Subscription;
    type Acker = T::Acker;
    type Cursor = T::Cursor;
    type Stream = T::Stream;

    fn read(
        &self,
        subscription: Self::Subscription,
    ) -> impl Future<Output = Result<Self::Stream>> + Send {
        (**self).read(subscription)
    }
}

pub trait DynReader<S, C, A: Acker = BoxAcker>: Send + Sync
where
    S: Send + 'static,
    C: Send + 'static,
{
    fn read_dyn<'a>(&'a self, subscription: S) -> BoxFuture<'a, Result<BoxStream<C, A>>>;
}

struct DynReaderAdapter<R>(R);

impl<R> DynReader<R::Subscription, R::Cursor, BoxAcker> for DynReaderAdapter<R>
where
    R: Reader + Send + Sync + 'static,
    R::Subscription: Send + 'static,
    R::Acker: 'static,
    R::Cursor: Send + 'static,
    R::Stream: 'static,
{
    fn read_dyn<'a>(
        &'a self,
        subscription: R::Subscription,
    ) -> BoxFuture<'a, Result<BoxStream<R::Cursor, BoxAcker>>> {
        Box::pin(async move {
            let stream = Reader::read(&self.0, subscription).await?;
            let erased: BoxStream<R::Cursor, BoxAcker> = Box::pin(
                stream.map(|res| res.map(|msg| msg.map_acker(|a| Box::new(a) as BoxAcker))),
            );
            Ok(erased)
        })
    }
}

pub type BoxReader<S, C, A = BoxAcker> = Box<dyn DynReader<S, C, A>>;
pub type ArcReader<S, C, A = BoxAcker> = Arc<dyn DynReader<S, C, A>>;

impl<S, C, A> Reader for dyn DynReader<S, C, A> + '_
where
    S: Send + 'static,
    C: Send + 'static,
    A: Acker + 'static,
{
    type Subscription = S;
    type Acker = A;
    type Cursor = C;
    type Stream = BoxStream<C, A>;

    fn read(
        &self,
        subscription: Self::Subscription,
    ) -> impl Future<Output = Result<Self::Stream>> + Send {
        DynReader::read_dyn(self, subscription)
    }
}

pub trait ReaderExt: Reader + Send + Sync + Sized + 'static
where
    Self::Subscription: Send + 'static,
    Self::Acker: 'static,
    Self::Cursor: Send + 'static,
    Self::Stream: 'static,
{
    fn into_boxed(self) -> BoxReader<Self::Subscription, Self::Cursor> {
        Box::new(DynReaderAdapter(self))
    }

    fn into_arced(self) -> ArcReader<Self::Subscription, Self::Cursor> {
        Arc::new(DynReaderAdapter(self))
    }
}

impl<R> ReaderExt for R
where
    R: Reader + Send + Sync + 'static,
    R::Subscription: Send + 'static,
    R::Acker: 'static,
    R::Cursor: Send + 'static,
    R::Stream: 'static,
{
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::stream;

    use crate::event::Event;
    use crate::io::Message;
    use crate::io::NoCursor;
    use crate::io::acker::NoopAcker;
    use crate::payload::Payload;

    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    struct TestCursor(i64);

    #[derive(Debug, Clone, Default)]
    struct TestSub;

    struct UnitReader;

    impl Reader for UnitReader {
        type Subscription = TestSub;
        type Acker = NoopAcker;
        type Cursor = TestCursor;
        type Stream = Pin<Box<dyn Stream<Item = Result<Message<NoopAcker, TestCursor>>> + Send>>;

        async fn read(&self, _: Self::Subscription) -> Result<Self::Stream> {
            let event =
                Event::create("org", "/x", "thing.happened", Payload::from_string("p")).unwrap();
            let msg = Message::new(event, NoopAcker, TestCursor(1));
            Ok(Box::pin(stream::once(async move { Ok(msg) })))
        }
    }

    #[tokio::test]
    async fn boxed_reader_preserves_cursor_type() {
        let reader: BoxReader<TestSub, TestCursor> = UnitReader.into_boxed();
        let mut stream = reader.read(TestSub).await.unwrap();
        let msg = stream.next().await.unwrap().unwrap();
        assert_eq!(*msg.cursor(), TestCursor(1));
    }

    #[tokio::test]
    async fn into_boxed_yields_dyn_safe_reader() {
        let reader: BoxReader<TestSub, TestCursor> = UnitReader.into_boxed();
        let mut stream = reader.read(TestSub).await.unwrap();
        let msg = stream.next().await.unwrap().unwrap();
        msg.ack().await.unwrap();
    }

    #[tokio::test]
    async fn into_arced_yields_shared_reader() {
        let reader: ArcReader<TestSub, TestCursor> = UnitReader.into_arced();
        let clone = Arc::clone(&reader);
        let mut stream = clone.read(TestSub).await.unwrap();
        let msg = stream.next().await.unwrap().unwrap();
        msg.ack().await.unwrap();
    }

    #[tokio::test]
    async fn vec_of_boxed_readers_dispatches_each() {
        let readers: Vec<BoxReader<TestSub, TestCursor>> =
            vec![UnitReader.into_boxed(), UnitReader.into_boxed()];
        for r in &readers {
            let mut stream = r.read(TestSub).await.unwrap();
            let msg = stream.next().await.unwrap().unwrap();
            msg.ack().await.unwrap();
        }
    }

    fn _assert_box_passes_as_generic_reader() {
        fn _take<R: Reader>(_: R) {}
        let r: BoxReader<TestSub, TestCursor> = UnitReader.into_boxed();
        _take(r);
    }

    fn _assert_reader_dyn_safe() {
        fn _take(_: BoxReader<TestSub, NoCursor>) {}
    }
}

pub mod batch;
pub mod checkpoint;
pub mod concurrency_limit;
pub mod dedupe;
pub mod filtered;
pub mod inspect;
pub mod map;
pub mod merge;
pub mod partitioned;
pub mod rate_limit;
pub mod recover;
pub mod replay_then_live;
pub mod timeout;
pub mod try_map;
pub mod watermark;
pub mod window;

pub use batch::{BatchAcker, BatchCursor, BatchReader};
pub use checkpoint::{
    CheckpointAcker, CheckpointKey, CheckpointReader, CheckpointReaderConfig, CheckpointScope,
    CheckpointStore, CheckpointStream, CheckpointSubscription, InvalidCursorPolicy,
    MissingCheckpointPolicy,
};
pub use concurrency_limit::{ConcurrencyLimitReader, LimitAcker};
pub use dedupe::{DedupeAcker, DedupeReader, DedupeStore, InMemoryDedupeStore};
pub use filtered::{FilteredReader, FilteredStream};
pub use inspect::{InspectAcker, InspectHooks, InspectReader, InspectStream};
pub use map::{MapReader, MapStream};
pub use merge::{MergeAcker, MergeCursor, MergeReader, MergeStrategy};
pub use partitioned::{
    LaneScheduling, PartitionAcker, PartitionedCursor, PartitionedReader, PartitionedReaderConfig,
    PartitionedSubscription,
};
pub use rate_limit::{RateLimit, RateLimitReader};
pub use recover::{RecoverConfig, RecoverReader};
pub use replay_then_live::{
    ReplayLiveAcker, ReplayLiveCursor, ReplayThenLiveConfig, ReplayThenLiveReader,
    ReplayThenLiveStream, ReplayThenLiveSubscription,
};
pub use timeout::{TimeoutAcker, TimeoutReader, TimeoutStream};
pub use try_map::{TryMapReader, TryMapStream};
pub use watermark::{WatermarkAcker, WatermarkReader, WatermarkStore};
pub use window::WindowReader;
