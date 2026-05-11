use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::{Stream, StreamExt};

use crate::EventSubscription;
use crate::error::Result;
use crate::io::message::Message;
use crate::io::{Acker, BoxAcker};

pub type BoxStream<A = BoxAcker> = Pin<Box<dyn Stream<Item = Result<Message<A>>> + Send>>;

pub trait Reader: Send + Sync {
    type Subscription: Send;
    type Acker: Acker;
    type Stream: Stream<Item = Result<Message<Self::Acker>>> + Send;

    fn read(
        &self,
        subscription: Self::Subscription,
    ) -> impl Future<Output = Result<Self::Stream>> + Send;
}

impl<T: Reader + ?Sized> Reader for Arc<T> {
    type Subscription = T::Subscription;
    type Acker = T::Acker;
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
    type Stream = T::Stream;

    fn read(
        &self,
        subscription: Self::Subscription,
    ) -> impl Future<Output = Result<Self::Stream>> + Send {
        (**self).read(subscription)
    }
}

pub trait DynReader<S: Send + 'static = EventSubscription, A: Acker = BoxAcker>:
    Send + Sync
{
    fn read_dyn<'a>(&'a self, subscription: S) -> BoxFuture<'a, Result<BoxStream<A>>>;
}

struct DynReaderAdapter<R>(R);

impl<R> DynReader<R::Subscription, BoxAcker> for DynReaderAdapter<R>
where
    R: Reader + Send + Sync + 'static,
    R::Subscription: Send + 'static,
    R::Acker: 'static,
    R::Stream: 'static,
{
    fn read_dyn<'a>(
        &'a self,
        subscription: R::Subscription,
    ) -> BoxFuture<'a, Result<BoxStream<BoxAcker>>> {
        Box::pin(async move {
            let stream = Reader::read(&self.0, subscription).await?;
            let erased: BoxStream<BoxAcker> = Box::pin(
                stream.map(|res| res.map(|msg| msg.map_acker(|a| Box::new(a) as BoxAcker))),
            );
            Ok(erased)
        })
    }
}

pub type BoxReader<S = EventSubscription, A = BoxAcker> = Box<dyn DynReader<S, A>>;
pub type ArcReader<S = EventSubscription, A = BoxAcker> = Arc<dyn DynReader<S, A>>;

impl<S, A> Reader for dyn DynReader<S, A> + '_
where
    S: Send + 'static,
    A: Acker + 'static,
{
    type Subscription = S;
    type Acker = A;
    type Stream = BoxStream<A>;

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
    Self::Stream: 'static,
{
    fn into_boxed(self) -> BoxReader<Self::Subscription> {
        Box::new(DynReaderAdapter(self))
    }

    fn into_arced(self) -> ArcReader<Self::Subscription> {
        Arc::new(DynReaderAdapter(self))
    }
}

impl<R> ReaderExt for R
where
    R: Reader + Send + Sync + 'static,
    R::Subscription: Send + 'static,
    R::Acker: 'static,
    R::Stream: 'static,
{
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::stream;

    use crate::event::Event;
    use crate::io::Message;
    use crate::io::acker::NoopAcker;
    use crate::payload::Payload;
    use crate::{EventSubscription, OrganizationId};

    struct UnitReader;

    impl Reader for UnitReader {
        type Subscription = EventSubscription;
        type Acker = NoopAcker;
        type Stream = Pin<Box<dyn Stream<Item = Result<Message<NoopAcker>>> + Send>>;

        async fn read(&self, _: Self::Subscription) -> Result<Self::Stream> {
            let event =
                Event::create("org", "/x", "thing.happened", Payload::from_string("p")).unwrap();
            let msg = Message::new(event, NoopAcker);
            Ok(Box::pin(stream::once(async move { Ok(msg) })))
        }
    }

    #[tokio::test]
    async fn into_boxed_yields_dyn_safe_reader() {
        let reader: BoxReader<EventSubscription> = UnitReader.into_boxed();
        let mut stream = reader
            .read(EventSubscription::new(OrganizationId::new("org").unwrap()))
            .await
            .unwrap();
        let msg = stream.next().await.unwrap().unwrap();
        msg.ack().await.unwrap();
    }

    #[tokio::test]
    async fn into_arced_yields_shared_reader() {
        let reader: ArcReader<EventSubscription> = UnitReader.into_arced();
        let clone = Arc::clone(&reader);
        let mut stream = clone
            .read(EventSubscription::new(OrganizationId::new("org").unwrap()))
            .await
            .unwrap();
        let msg = stream.next().await.unwrap().unwrap();
        msg.ack().await.unwrap();
    }

    #[tokio::test]
    async fn vec_of_boxed_readers_dispatches_each() {
        let readers: Vec<BoxReader<EventSubscription>> =
            vec![UnitReader.into_boxed(), UnitReader.into_boxed()];
        for r in &readers {
            let mut stream = r
                .read(EventSubscription::new(OrganizationId::new("org").unwrap()))
                .await
                .unwrap();
            let msg = stream.next().await.unwrap().unwrap();
            msg.ack().await.unwrap();
        }
    }

    fn _assert_box_passes_as_generic_reader() {
        fn _take<R: Reader>(_: R) {}
        let r: BoxReader<EventSubscription> = UnitReader.into_boxed();
        _take(r);
    }

    fn _assert_reader_dyn_safe() {
        fn _take(_: BoxReader) {}
    }
}
