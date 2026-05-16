use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Stream;

use crate::error::{Error, Result};
use crate::event::Event;
use crate::io::{Acker, Message, Reader};

pub trait InspectHooks: Send + Sync {
    fn on_deliver(&self, _event: &Event) {}
    fn on_ack(&self) {}
    fn on_nack(&self) {}
    fn on_error(&self, _error: &Error) {}
}

impl<A: Acker, H: InspectHooks> Acker for InspectAcker<A, H> {
    async fn ack(&self) -> Result<()> {
        self.hooks.on_ack();
        self.inner.ack().await
    }

    async fn nack(&self) -> Result<()> {
        self.hooks.on_nack();
        self.inner.nack().await
    }
}

pub struct InspectAcker<A: Acker, H: InspectHooks> {
    inner: A,
    hooks: Arc<H>,
}

pub struct InspectReader<R, H> {
    inner: R,
    hooks: Arc<H>,
}

impl<R, H> InspectReader<R, H> {
    pub fn new(inner: R, hooks: H) -> Self {
        Self {
            inner,
            hooks: Arc::new(hooks),
        }
    }
}

impl<R, H> Reader for InspectReader<R, H>
where
    R: Reader + Send + Sync + 'static,
    R::Subscription: Send + 'static,
    R::Acker: Clone + Send + Sync + 'static,
    R::Cursor: Send + Sync + 'static,
    R::Stream: 'static,
    H: InspectHooks + 'static,
{
    type Subscription = R::Subscription;
    type Acker = R::Acker;
    type Cursor = R::Cursor;
    type Stream = InspectStream<R, H>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let inner = self.inner.read(subscription).await?;
        Ok(InspectStream {
            inner: Box::pin(inner),
            hooks: Arc::clone(&self.hooks),
        })
    }
}

pub struct InspectStream<R: Reader, H> {
    inner: Pin<Box<R::Stream>>,
    hooks: Arc<H>,
}

impl<R, H> Stream for InspectStream<R, H>
where
    R: Reader,
    R::Acker: Clone,
    H: InspectHooks,
{
    type Item = Result<Message<R::Acker, R::Cursor>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(msg))) => {
                self.hooks.on_deliver(msg.event());
                Poll::Ready(Some(Ok(msg)))
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
    use crate::event::Event;
    use crate::io::acker::NoopAcker;
    use crate::io::{Cursor, Reader};
    use crate::payload::Payload;

    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    struct TestCursor(u64);

    impl Cursor for TestCursor {}

    struct VecReader {
        items: Mutex<Option<Vec<Result<Message<NoopAcker, TestCursor>>>>>,
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

    #[derive(Clone)]
    struct CountingHooks {
        delivers: Arc<AtomicUsize>,
        acks: Arc<AtomicUsize>,
        nacks: Arc<AtomicUsize>,
        errors: Arc<AtomicUsize>,
    }

    impl CountingHooks {
        fn new() -> Self {
            Self {
                delivers: Arc::new(AtomicUsize::new(0)),
                acks: Arc::new(AtomicUsize::new(0)),
                nacks: Arc::new(AtomicUsize::new(0)),
                errors: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl InspectHooks for CountingHooks {
        fn on_deliver(&self, _: &Event) {
            self.delivers.fetch_add(1, Ordering::SeqCst);
        }
        fn on_ack(&self) {
            self.acks.fetch_add(1, Ordering::SeqCst);
        }
        fn on_nack(&self) {
            self.nacks.fetch_add(1, Ordering::SeqCst);
        }
        fn on_error(&self, _: &Error) {
            self.errors.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn ev() -> Event {
        Event::create("org", "/x", "thing.happened", Payload::from_string("p")).unwrap()
    }

    #[tokio::test]
    async fn inspect_reader_calls_on_deliver() {
        let hooks = CountingHooks::new();
        let reader = VecReader {
            items: Mutex::new(Some(vec![Ok(Message::new(ev(), NoopAcker, TestCursor(1)))])),
        };
        let inspect = InspectReader::new(reader, hooks);
        let mut stream = inspect.read(()).await.unwrap();
        let msg = stream.next().await.unwrap().unwrap();
        assert_eq!(msg.event().topic().as_str(), "thing.happened");
        msg.ack().await.unwrap();
    }

    #[tokio::test]
    async fn inspect_reader_counts_delivered() {
        let hooks = CountingHooks::new();
        let reader = VecReader {
            items: Mutex::new(Some(vec![Ok(Message::new(ev(), NoopAcker, TestCursor(1)))])),
        };
        let inspect = InspectReader::new(reader, hooks.clone());
        let mut stream = inspect.read(()).await.unwrap();
        let msg = stream.next().await.unwrap().unwrap();
        assert_eq!(hooks.delivers.load(Ordering::SeqCst), 1);
        msg.ack().await.unwrap();
        assert_eq!(hooks.acks.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn inspect_reader_calls_on_error() {
        let hooks = CountingHooks::new();
        let reader = VecReader {
            items: Mutex::new(Some(vec![Err(Error::Store("fail".into()))])),
        };
        let inspect = InspectReader::new(reader, hooks.clone());
        let mut stream = inspect.read(()).await.unwrap();
        let _ = stream.next().await.unwrap();
        assert_eq!(hooks.errors.load(Ordering::SeqCst), 1);
    }
}
