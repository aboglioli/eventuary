use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Stream;

use crate::error::Result;
use crate::event::Event;
use crate::io::{Message, Reader};

pub struct TryMapReader<R, F> {
    inner: R,
    f: Arc<F>,
}

impl<R, F> TryMapReader<R, F> {
    pub fn new(inner: R, f: F) -> Self {
        Self {
            inner,
            f: Arc::new(f),
        }
    }
}

impl<R, F> Reader for TryMapReader<R, F>
where
    R: Reader + Send + Sync + 'static,
    R::Subscription: Send + 'static,
    R::Acker: Send + Sync + 'static,
    R::Cursor: Send + Sync + 'static,
    R::Stream: 'static,
    F: Fn(Event) -> Result<Event> + Send + Sync + 'static,
{
    type Subscription = R::Subscription;
    type Acker = R::Acker;
    type Cursor = R::Cursor;
    type Stream = TryMapStream<R, F>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let inner = self.inner.read(subscription).await?;
        Ok(TryMapStream {
            inner: Box::pin(inner),
            f: Arc::clone(&self.f),
        })
    }
}

pub struct TryMapStream<R: Reader, F> {
    inner: Pin<Box<R::Stream>>,
    f: Arc<F>,
}

impl<R, F> Stream for TryMapStream<R, F>
where
    R: Reader,
    F: Fn(Event) -> Result<Event>,
{
    type Item = Result<Message<R::Acker, R::Cursor>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(msg))) => {
                let (event, acker, cursor) = msg.into_parts();
                match (self.f)(event) {
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
        Event::create("org", "/x", topic, Payload::from_string("p")).unwrap()
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
            Ok(Event::create("org", "/x", "out", Payload::from_string("p")).unwrap())
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
