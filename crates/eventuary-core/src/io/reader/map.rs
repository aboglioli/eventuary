use std::marker::Unpin;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;

use crate::error::Result;
use crate::event::Event;
use crate::io::{Message, Reader};

pub struct MapReader<R, F> {
    inner: R,
    f: F,
}

impl<R, F> MapReader<R, F> {
    pub fn new(inner: R, f: F) -> Self {
        Self { inner, f }
    }
}

impl<R, F> Reader for MapReader<R, F>
where
    R: Reader + Send + Sync + 'static,
    R::Subscription: Send + 'static,
    R::Acker: Send + Sync + 'static,
    R::Cursor: Send + Sync + 'static,
    R::Stream: 'static,
    F: Fn(Event) -> Event + Clone + Unpin + Send + Sync + 'static,
{
    type Subscription = R::Subscription;
    type Acker = R::Acker;
    type Cursor = R::Cursor;
    type Stream = MapStream<R, F>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let inner = self.inner.read(subscription).await?;
        Ok(MapStream {
            inner: Box::pin(inner),
            f: self.f.clone(),
        })
    }
}

pub struct MapStream<R: Reader, F> {
    inner: Pin<Box<R::Stream>>,
    f: F,
}

impl<R, F> Stream for MapStream<R, F>
where
    R: Reader,
    F: Fn(Event) -> Event + Unpin,
{
    type Item = Result<Message<R::Acker, R::Cursor>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(msg))) => {
                let f = &self.f;
                let mapped = msg.map_event(f);
                Poll::Ready(Some(Ok(mapped)))
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
    use crate::event::Event;
    use crate::io::acker::NoopAcker;
    use crate::io::{Message, Reader};
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
    async fn map_reader_transforms_topic() {
        let reader = VecReader {
            items: Mutex::new(Some(vec![Ok(Message::new(
                ev("a.b"),
                NoopAcker,
                TestCursor(1),
            ))])),
        };
        let mapped = MapReader::new(reader, |_event: Event| {
            Event::create("org", "/x", "mapped.topic", Payload::from_string("mapped")).unwrap()
        });
        let mut stream = mapped.read(()).await.unwrap();
        let msg = stream.next().await.unwrap().unwrap();
        assert_eq!(msg.event().topic().as_str(), "mapped.topic");
    }

    #[tokio::test]
    async fn map_reader_preserves_acker_and_cursor() {
        let reader = VecReader {
            items: Mutex::new(Some(vec![Ok(Message::new(
                ev("a.b"),
                NoopAcker,
                TestCursor(42),
            ))])),
        };
        let mapped = MapReader::new(reader, |e| e);
        let mut stream = mapped.read(()).await.unwrap();
        let msg = stream.next().await.unwrap().unwrap();
        assert_eq!(*msg.cursor(), TestCursor(42));
        msg.ack().await.unwrap();
    }

    #[tokio::test]
    async fn map_reader_forwards_inner_errors() {
        let reader = VecReader {
            items: Mutex::new(Some(vec![Err(crate::error::Error::Store(
                "inner failed".into(),
            ))])),
        };
        let mapped = MapReader::new(reader, |e| e);
        let mut stream = mapped.read(()).await.unwrap();
        let err = match stream.next().await.unwrap() {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        };
        assert!(err.to_string().contains("inner failed"));
    }
}
