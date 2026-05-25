use std::marker::{PhantomData, Unpin};
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;

use crate::error::Result;
use crate::event::Event;
use crate::io::{Message, Reader};
use crate::payload::Payload;

pub struct MapReader<R, F, P = Payload, Q = Payload> {
    inner: R,
    f: F,
    _payload: PhantomData<fn(P) -> Q>,
}

impl<R, F, P, Q> MapReader<R, F, P, Q> {
    pub fn new(inner: R, f: F) -> Self {
        Self {
            inner,
            f,
            _payload: PhantomData,
        }
    }
}

impl<R, F, P, Q> Reader<Q> for MapReader<R, F, P, Q>
where
    R: Reader<P> + Send + Sync + 'static,
    R::Subscription: Send + 'static,
    R::Acker: Send + Sync + 'static,
    R::Cursor: Send + Sync + 'static,
    R::Stream: 'static,
    F: Fn(Event<P>) -> Event<Q> + Clone + Unpin + Send + Sync + 'static,
    P: Send + 'static,
    Q: Send + 'static,
{
    type Subscription = R::Subscription;
    type Acker = R::Acker;
    type Cursor = R::Cursor;
    type Stream = MapStream<R, F, P, Q>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let inner = self.inner.read(subscription).await?;
        Ok(MapStream {
            inner: Box::pin(inner),
            f: self.f.clone(),
            _payload: PhantomData,
        })
    }
}

pub struct MapStream<R: Reader<P>, F, P = Payload, Q = Payload> {
    inner: Pin<Box<R::Stream>>,
    f: F,
    _payload: PhantomData<fn(P) -> Q>,
}

impl<R, F, P, Q> Stream for MapStream<R, F, P, Q>
where
    R: Reader<P>,
    F: Fn(Event<P>) -> Event<Q> + Unpin,
{
    type Item = Result<Message<R::Acker, R::Cursor, Q>>;

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
    use crate::error::Error;
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
        Event::create("org", "/x", topic, "thing-1", Payload::from_string("p")).unwrap()
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
            Event::create(
                "org",
                "/x",
                "mapped.topic",
                "thing-1",
                Payload::from_string("mapped"),
            )
            .unwrap()
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
            items: Mutex::new(Some(vec![Err(Error::Store("inner failed".into()))])),
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
