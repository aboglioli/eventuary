use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::{Stream, StreamExt};

use crate::error::Result;
use crate::io::message::Message;
use crate::io::{Acker, BoxAcker};
use crate::payload::Payload;

pub type BoxStream<C, A = BoxAcker, P = Payload> =
    Pin<Box<dyn Stream<Item = Result<Message<A, C, P>>> + Send>>;

pub trait Reader<P = Payload>: Send + Sync {
    type Subscription: Send;
    type Acker: Acker;
    type Cursor: Send;
    type Stream: Stream<Item = Result<Message<Self::Acker, Self::Cursor, P>>> + Send;

    fn read(
        &self,
        subscription: Self::Subscription,
    ) -> impl Future<Output = Result<Self::Stream>> + Send;
}

impl<T: Reader<P> + ?Sized, P> Reader<P> for Arc<T> {
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

impl<T: Reader<P> + ?Sized, P> Reader<P> for Box<T> {
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

pub trait DynReader<S, C, A: Acker = BoxAcker, P = Payload>: Send + Sync
where
    S: Send + 'static,
    C: Send + 'static,
    P: Send + 'static,
{
    fn read_dyn<'a>(&'a self, subscription: S) -> BoxFuture<'a, Result<BoxStream<C, A, P>>>;
}

struct DynReaderAdapter<R>(R);

impl<R, P> DynReader<R::Subscription, R::Cursor, BoxAcker, P> for DynReaderAdapter<R>
where
    R: Reader<P> + Send + Sync + 'static,
    R::Subscription: Send + 'static,
    R::Acker: Acker + 'static,
    R::Cursor: Send + 'static,
    R::Stream: 'static,
    P: Send + 'static,
{
    fn read_dyn<'a>(
        &'a self,
        subscription: R::Subscription,
    ) -> BoxFuture<'a, Result<BoxStream<R::Cursor, BoxAcker, P>>> {
        Box::pin(async move {
            let stream = Reader::<P>::read(&self.0, subscription).await?;
            let erased: BoxStream<R::Cursor, BoxAcker, P> = Box::pin(
                stream.map(|res| res.map(|msg| msg.map_acker(|a| Box::new(a) as BoxAcker))),
            );
            Ok(erased)
        })
    }
}

pub type BoxReader<S, C, A = BoxAcker, P = Payload> = Box<dyn DynReader<S, C, A, P>>;
pub type ArcReader<S, C, A = BoxAcker, P = Payload> = Arc<dyn DynReader<S, C, A, P>>;

impl<S, C, A, P> Reader<P> for dyn DynReader<S, C, A, P> + '_
where
    S: Send + 'static,
    C: Send + 'static,
    A: Acker + 'static,
    P: Send + 'static,
{
    type Subscription = S;
    type Acker = A;
    type Cursor = C;
    type Stream = BoxStream<C, A, P>;

    fn read(
        &self,
        subscription: Self::Subscription,
    ) -> impl Future<Output = Result<Self::Stream>> + Send {
        DynReader::read_dyn(self, subscription)
    }
}

pub trait ReaderExt<P = Payload>: Reader<P> + Send + Sync + Sized + 'static
where
    Self::Subscription: Send + 'static,
    Self::Acker: 'static,
    Self::Cursor: Send + 'static,
    Self::Stream: 'static,
    P: Send + 'static,
{
    fn into_boxed(self) -> BoxReader<Self::Subscription, Self::Cursor, BoxAcker, P> {
        Box::new(DynReaderAdapter(self))
    }

    fn into_arced(self) -> ArcReader<Self::Subscription, Self::Cursor, BoxAcker, P> {
        Arc::new(DynReaderAdapter(self))
    }
}

impl<R, P> ReaderExt<P> for R
where
    R: Reader<P> + Send + Sync + 'static,
    R::Subscription: Send + 'static,
    R::Acker: 'static,
    R::Cursor: Send + 'static,
    R::Stream: 'static,
    P: Send + 'static,
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
            let event = Event::create(
                "org",
                "/x",
                "thing.happened",
                crate::payload::Payload::from_string("p"),
            )
            .unwrap();
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

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct UserUpdated {
        user_id: String,
    }

    struct TypedUnitReader;

    impl Reader<UserUpdated> for TypedUnitReader {
        type Subscription = TestSub;
        type Acker = NoopAcker;
        type Cursor = TestCursor;
        type Stream =
            Pin<Box<dyn Stream<Item = Result<Message<NoopAcker, TestCursor, UserUpdated>>> + Send>>;

        async fn read(&self, _: Self::Subscription) -> Result<Self::Stream> {
            let event = Event::create(
                "org",
                "/users",
                "user.updated",
                UserUpdated {
                    user_id: "u-1".to_owned(),
                },
            )
            .unwrap();
            let msg = Message::new(event, NoopAcker, TestCursor(1));
            Ok(Box::pin(stream::once(async move { Ok(msg) })))
        }
    }

    #[tokio::test]
    async fn typed_reader_into_boxed_yields_dyn_safe_reader() {
        let reader: BoxReader<TestSub, TestCursor, BoxAcker, UserUpdated> =
            TypedUnitReader.into_boxed();
        let mut stream = reader.read(TestSub).await.unwrap();
        let msg = stream.next().await.unwrap().unwrap();
        assert_eq!(msg.event().payload().user_id, "u-1");
    }
}

pub mod batch;
pub mod buffer;
pub mod checkpoint;
pub mod claim_buffer;
pub mod concurrency_limit;
pub mod coordinated;
pub mod decode;
pub mod dedupe;
pub mod encoded_cursor;
pub mod filtered;
pub mod inspect;
pub mod map;
pub mod merge;
pub mod outcome_router;
pub mod partitioned;
pub mod rate_limit;
pub mod recover;
pub mod replay_then_live;
pub mod timeout;
pub mod try_map;
pub mod watermark;
pub mod window;

pub use batch::{BatchAcker, BatchCursor, BatchReader};
pub use buffer::{BufferAcker, BufferEntry, BufferStore, BufferedReader, BufferedReaderConfig};
pub use checkpoint::{
    CheckpointAcker, CheckpointKey, CheckpointReader, CheckpointReaderConfig, CheckpointScope,
    CheckpointStore, CheckpointStream, CheckpointSubscription, InvalidCursorPolicy,
    MissingCheckpointPolicy,
};
pub use claim_buffer::{ClaimedBufferEntry, ClaimedBufferStore};
pub use concurrency_limit::{ConcurrencyLimitReader, LimitAcker};
pub use coordinated::{
    CoordinatedAcker, CoordinatedCursor, CoordinatedReader, CoordinatedReaderConfig,
    CoordinatedStream, CoordinatedSubscription,
};
pub use decode::{DecodeErrorDisposition, DecodeReader, ReaderTypedExt};
pub use dedupe::{DedupeAcker, DedupeReader, DedupeStore};
pub use encoded_cursor::{EncodedCursorReader, EncodedCursorSubscription};
pub use filtered::{FilteredReader, FilteredStream};
pub use inspect::{InspectAcker, InspectHooks, InspectReader, InspectStream};
pub use map::{MapReader, MapStream};
pub use merge::{MergeAcker, MergeCursor, MergeReader, MergeStrategy};
pub use outcome_router::{
    DeliveryDisposition, NackDisposition, OutcomeRouterAcker, OutcomeRouterReader,
};
pub use partitioned::{
    LaneScheduling, PartitionAcker, PartitionRouteStrategy, PartitionedCursor, PartitionedReader,
    PartitionedReaderConfig, PartitionedSubscription,
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
