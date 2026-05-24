use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Stream;

use crate::error::{Error, Result};
use crate::event::Event;
use crate::io::acker::NackContext;
use crate::io::{Acker, Message, Reader};
use crate::payload::Payload;

pub trait InspectHooks<P = Payload>: Send + Sync {
    fn on_deliver(&self, _event: &Event<P>) {}
    fn on_ack(&self, _event: &Event<P>) {}
    fn on_nack(&self, _event: &Event<P>) {}
    fn on_nack_with(&self, event: &Event<P>, _context: &NackContext) {
        self.on_nack(event);
    }
    fn on_error(&self, _error: &Error) {}
}

pub struct InspectReader<R, H, P = Payload> {
    inner: R,
    hooks: Arc<H>,
    _payload: PhantomData<fn(P)>,
}

impl<R, H, P> InspectReader<R, H, P> {
    pub fn new(inner: R, hooks: H) -> Self {
        Self {
            inner,
            hooks: Arc::new(hooks),
            _payload: PhantomData,
        }
    }
}

impl<R, H, P> Reader<P> for InspectReader<R, H, P>
where
    R: Reader<P> + Send + Sync + 'static,
    R::Subscription: Send + 'static,
    R::Acker: Send + Sync + 'static,
    R::Cursor: Send + Sync + 'static,
    R::Stream: 'static,
    H: InspectHooks<P> + 'static,
    P: Clone + Send + Sync + 'static,
{
    type Subscription = R::Subscription;
    type Acker = InspectAcker<R::Acker, H, P>;
    type Cursor = R::Cursor;
    type Stream = InspectStream<R, H, P>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let inner = self.inner.read(subscription).await?;
        Ok(InspectStream {
            inner: Box::pin(inner),
            hooks: Arc::clone(&self.hooks),
            _payload: PhantomData,
        })
    }
}

pub struct InspectAcker<A: Acker, H, P = Payload> {
    inner: A,
    hooks: Arc<H>,
    event: Arc<Event<P>>,
}

impl<A, H, P> Acker for InspectAcker<A, H, P>
where
    A: Acker,
    H: InspectHooks<P>,
    P: Send + Sync,
{
    async fn ack(&self) -> Result<()> {
        self.hooks.on_ack(&self.event);
        self.inner.ack().await
    }

    async fn nack(&self) -> Result<()> {
        self.hooks.on_nack(&self.event);
        self.inner.nack().await
    }

    async fn nack_with(&self, context: NackContext) -> Result<()> {
        self.hooks.on_nack_with(&self.event, &context);
        self.inner.nack_with(context).await
    }
}

pub struct InspectStream<R: Reader<P>, H, P = Payload> {
    inner: Pin<Box<R::Stream>>,
    hooks: Arc<H>,
    _payload: PhantomData<fn(P)>,
}

impl<R, H, P> Stream for InspectStream<R, H, P>
where
    R: Reader<P>,
    R::Acker: Send + Sync,
    H: InspectHooks<P>,
    P: Clone + Send + Sync,
{
    type Item = Result<Message<InspectAcker<R::Acker, H, P>, R::Cursor, P>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(msg))) => {
                self.hooks.on_deliver(msg.event());
                let (event, inner_acker, cursor) = msg.into_parts();
                let event_arc = Arc::new(event);
                let acker = InspectAcker {
                    inner: inner_acker,
                    hooks: Arc::clone(&self.hooks),
                    event: Arc::clone(&event_arc),
                };
                let out_event = (*event_arc).clone();
                Poll::Ready(Some(Ok(Message::new(out_event, acker, cursor))))
            }
            Poll::Ready(Some(Err(e))) => {
                self.hooks.on_error(&e);
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::{Stream, StreamExt, stream};

    use super::*;
    use crate::event::{Event, EventId};
    use crate::io::Reader;
    use crate::io::acker::NoopAcker;
    use crate::payload::Payload;

    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    struct TestCursor(u64);

    type TestItems = Mutex<Option<Vec<Result<Message<NoopAcker, TestCursor>>>>>;

    struct VecReader {
        items: TestItems,
    }

    impl Reader for VecReader {
        type Subscription = ();
        type Acker = NoopAcker;
        type Cursor = TestCursor;
        type Stream = Pin<Box<dyn Stream<Item = Result<Message<NoopAcker, TestCursor>>> + Send>>;

        async fn read(&self, _: ()) -> Result<Self::Stream> {
            let items = self.items.lock().unwrap().take().unwrap_or_default();
            Ok(Box::pin(stream::iter(items)))
        }
    }

    #[derive(Clone, Default)]
    struct RecordingHooks {
        delivered: Arc<Mutex<Vec<EventId>>>,
        acked: Arc<Mutex<Vec<EventId>>>,
        nacked: Arc<Mutex<Vec<EventId>>>,
        errors: Arc<AtomicUsize>,
    }

    impl InspectHooks for RecordingHooks {
        fn on_deliver(&self, event: &Event) {
            self.delivered.lock().unwrap().push(event.id());
        }
        fn on_ack(&self, event: &Event) {
            self.acked.lock().unwrap().push(event.id());
        }
        fn on_nack(&self, event: &Event) {
            self.nacked.lock().unwrap().push(event.id());
        }
        fn on_error(&self, _: &Error) {
            self.errors.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn ev() -> Event {
        Event::create("org", "/x", "thing.happened", Payload::from_string("p")).unwrap()
    }

    #[tokio::test]
    async fn inspect_reader_passes_event_to_ack_hook() {
        let hooks = RecordingHooks::default();
        let event = ev();
        let event_id = event.id();
        let reader = VecReader {
            items: Mutex::new(Some(vec![Ok(Message::new(
                event,
                NoopAcker,
                TestCursor(1),
            ))])),
        };
        let inspect = InspectReader::new(reader, hooks.clone());
        let mut stream = inspect.read(()).await.unwrap();
        let msg = stream.next().await.unwrap().unwrap();
        assert_eq!(hooks.delivered.lock().unwrap().as_slice(), &[event_id]);
        msg.ack().await.unwrap();
        assert_eq!(hooks.acked.lock().unwrap().as_slice(), &[event_id]);
    }

    #[tokio::test]
    async fn inspect_reader_passes_event_to_nack_hook() {
        let hooks = RecordingHooks::default();
        let event = ev();
        let event_id = event.id();
        let reader = VecReader {
            items: Mutex::new(Some(vec![Ok(Message::new(
                event,
                NoopAcker,
                TestCursor(1),
            ))])),
        };
        let inspect = InspectReader::new(reader, hooks.clone());
        let mut stream = inspect.read(()).await.unwrap();
        let msg = stream.next().await.unwrap().unwrap();
        msg.nack().await.unwrap();
        assert_eq!(hooks.nacked.lock().unwrap().as_slice(), &[event_id]);
        assert!(hooks.acked.lock().unwrap().is_empty());
    }

    #[derive(Clone, Default)]
    struct NackContextHooks {
        reasons: Arc<Mutex<Vec<crate::io::acker::NackReason>>>,
    }

    impl InspectHooks for NackContextHooks {
        fn on_nack_with(&self, _event: &Event, context: &NackContext) {
            self.reasons.lock().unwrap().push(context.reason());
        }
    }

    #[tokio::test]
    async fn inspect_reader_passes_nack_context_to_on_nack_with_hook() {
        use crate::io::acker::{NackContext, NackReason};
        let hooks = NackContextHooks::default();
        let reader = VecReader {
            items: Mutex::new(Some(vec![Ok(Message::new(ev(), NoopAcker, TestCursor(1)))])),
        };
        let inspect = InspectReader::new(reader, hooks.clone());
        let mut stream = inspect.read(()).await.unwrap();
        let msg = stream.next().await.unwrap().unwrap();
        let context = NackContext::delivery_expired("expired");
        msg.nack_with(context).await.unwrap();
        let reasons = hooks.reasons.lock().unwrap().clone();
        assert_eq!(reasons, vec![NackReason::DeliveryExpired]);
    }

    #[tokio::test]
    async fn inspect_reader_default_on_nack_with_delegates_to_on_nack() {
        use crate::io::acker::NackContext;
        let hooks = RecordingHooks::default();
        let event = ev();
        let event_id = event.id();
        let reader = VecReader {
            items: Mutex::new(Some(vec![Ok(Message::new(
                event,
                NoopAcker,
                TestCursor(1),
            ))])),
        };
        let inspect = InspectReader::new(reader, hooks.clone());
        let mut stream = inspect.read(()).await.unwrap();
        let msg = stream.next().await.unwrap().unwrap();
        msg.nack_with(NackContext::delivery_expired("expired"))
            .await
            .unwrap();
        assert_eq!(hooks.nacked.lock().unwrap().as_slice(), &[event_id]);
    }

    #[tokio::test]
    async fn inspect_reader_calls_on_error() {
        let hooks = RecordingHooks::default();
        let reader = VecReader {
            items: Mutex::new(Some(vec![Err(Error::Store("fail".into()))])),
        };
        let inspect = InspectReader::new(reader, hooks.clone());
        let mut stream = inspect.read(()).await.unwrap();
        let _ = stream.next().await.unwrap();
        assert_eq!(hooks.errors.load(Ordering::SeqCst), 1);
    }
}
