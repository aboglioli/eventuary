use std::marker::{PhantomData, Unpin};
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;

use crate::error::Result;
use crate::event::Event;
use crate::io::{Message, Reader};
use crate::payload::Payload;

pub struct TryMapReader<R, F, P = Payload, Q = Payload> {
    inner: R,
    f: F,
    _payload: PhantomData<fn(P) -> Q>,
}

impl<R, F, P, Q> TryMapReader<R, F, P, Q> {
    pub fn new(inner: R, f: F) -> Self {
        Self {
            inner,
            f,
            _payload: PhantomData,
        }
    }
}

impl<R, F, P, Q> Reader<Q> for TryMapReader<R, F, P, Q>
where
    R: Reader<P> + Send + Sync + 'static,
    R::Subscription: Send + 'static,
    R::Acker: Send + Sync + 'static,
    R::Cursor: Send + Sync + 'static,
    R::Stream: 'static,
    F: Fn(Event<P>) -> Result<Event<Q>> + Clone + Unpin + Send + Sync + 'static,
    P: Send + 'static,
    Q: Send + 'static,
{
    type Subscription = R::Subscription;
    type Acker = R::Acker;
    type Cursor = R::Cursor;
    type Stream = TryMapStream<R, F, P, Q>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let inner = self.inner.read(subscription).await?;
        Ok(TryMapStream {
            inner: Box::pin(inner),
            f: self.f.clone(),
            _payload: PhantomData,
        })
    }
}

pub struct TryMapStream<R: Reader<P>, F, P = Payload, Q = Payload> {
    inner: Pin<Box<R::Stream>>,
    f: F,
    _payload: PhantomData<fn(P) -> Q>,
}

impl<R, F, P, Q> Stream for TryMapStream<R, F, P, Q>
where
    R: Reader<P>,
    F: Fn(Event<P>) -> Result<Event<Q>> + Unpin,
{
    type Item = Result<Message<R::Acker, R::Cursor, Q>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(msg))) => {
                let (event, acker, cursor) = msg.into_parts();
                let f = &self.f;
                match f(event) {
                    Ok(mapped) => Poll::Ready(Some(Ok(Message::new(mapped, acker, cursor)))),
                    Err(e) => Poll::Ready(Some(Err(e))),
                }
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::Mutex;

    use futures::{Stream, StreamExt, stream};

    use super::*;
    use crate::error::Error;
    use crate::event::Event;
    use crate::io::Message;
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

    fn ev(topic: &str) -> Event {
        Event::create("org", "/x", topic, "thing-1", Payload::from_string("p")).unwrap()
    }

    #[tokio::test]
    async fn try_map_passes_ok() {
        let reader = VecReader {
            items: Mutex::new(Some(vec![Ok(Message::new(
                ev("a.b"),
                NoopAcker,
                TestCursor(1),
            ))])),
        };
        let mapped = TryMapReader::new(reader, |_event: Event| {
            Ok(Event::create("org", "/x", "out", "thing-1", Payload::from_string("p")).unwrap())
        });
        let mut stream = mapped.read(()).await.unwrap();
        let msg = stream.next().await.unwrap().unwrap();
        assert_eq!(msg.event().topic().as_str(), "out");
    }

    #[tokio::test]
    async fn try_map_propagates_err() {
        let reader = VecReader {
            items: Mutex::new(Some(vec![Ok(Message::new(
                ev("a.b"),
                NoopAcker,
                TestCursor(1),
            ))])),
        };
        let mapped = TryMapReader::new(reader, |_event: Event| -> Result<Event> {
            Err(Error::Store("mapping failed".into()))
        });
        let mut stream = mapped.read(()).await.unwrap();
        let result = stream.next().await.unwrap();
        assert!(result.is_err());
    }
}
