use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use crate::error::Result;
use crate::io::stream::SpawnedStream;
use crate::io::{Acker, Message, Reader};

pub struct ConcurrencyLimitReader<R> {
    inner: R,
    semaphore: Arc<Semaphore>,
}

impl<R> ConcurrencyLimitReader<R> {
    pub fn new(inner: R, limit: usize) -> Self {
        Self {
            inner,
            semaphore: Arc::new(Semaphore::new(limit)),
        }
    }
}

impl<R> Reader for ConcurrencyLimitReader<R>
where
    R: Reader + Send + Sync + 'static,
    R::Subscription: Send + 'static,
    R::Acker: Send + Sync + 'static,
    R::Cursor: Send + Sync + 'static,
    R::Stream: Send + 'static,
{
    type Subscription = R::Subscription;
    type Acker = LimitAcker<R::Acker>;
    type Cursor = R::Cursor;
    type Stream = SpawnedStream<LimitAcker<R::Acker>, R::Cursor>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let inner = self.inner.read(subscription).await?;
        let semaphore = Arc::clone(&self.semaphore);
        let (tx, rx) = mpsc::channel(64);

        let handle = tokio::spawn(async move {
            let mut inner = Box::pin(inner);
            while let Some(item) = inner.next().await {
                let permit = match Arc::clone(&semaphore).acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => return,
                };
                let mapped = match item {
                    Ok(msg) => {
                        let (event, acker, cursor) = msg.into_parts();
                        Some(Message::new(event, LimitAcker { inner: acker, _permit: permit }, cursor))
                    }
                    Err(_) => {
                        // Release permit immediately on error, no message to deliver
                        drop(permit);
                        None
                    }
                };
                if let Some(msg) = mapped {
                    if tx.send(Ok(msg)).await.is_err() { return; }
                }
            }
        });

        Ok(SpawnedStream::new(rx, handle))
    }
}

pub struct LimitAcker<A: Acker> {
    inner: A,
    _permit: OwnedSemaphorePermit,
}

impl<A: Acker> Acker for LimitAcker<A> {
    async fn ack(&self) -> Result<()> {
        self.inner.ack().await
    }

    async fn nack(&self) -> Result<()> {
        self.inner.nack().await
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::time::Duration;

    use futures::{Stream, StreamExt, stream};

    use super::*;
    use crate::event::Event;
    use crate::io::acker::NoopAcker;
    use crate::payload::Payload;

    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    struct TestCursor(u64);

    struct VecReader {
        events: Mutex<Option<Vec<Event>>>,
    }

    impl Reader for VecReader {
        type Subscription = ();
        type Acker = NoopAcker;
        type Cursor = TestCursor;
        type Stream = Pin<Box<dyn Stream<Item = Result<Message<NoopAcker, TestCursor>>> + Send>>;

        async fn read(&self, _: ()) -> Result<Self::Stream> {
            let events = self.events.lock().unwrap().take().unwrap_or_default();
            Ok(Box::pin(stream::iter(
                events.into_iter().enumerate().map(|(i, e)| {
                    Ok(Message::new(e, NoopAcker, TestCursor(i as u64)))
                }),
            )))
        }
    }

    fn ev() -> Event {
        Event::create("org", "/x", "thing.happened", Payload::from_string("p")).unwrap()
    }

    #[tokio::test]
    async fn limits_concurrent_in_flight() {
        let events: Vec<Event> = (0..5).map(|_| ev()).collect();
        let reader = VecReader {
            events: Mutex::new(Some(events)),
        };
        let limited = ConcurrencyLimitReader::new(reader, 2);
        let mut stream = limited.read(()).await.unwrap();

        // First 2 messages arrive immediately
        let m1 = tokio::time::timeout(Duration::from_millis(500), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let _m2 = tokio::time::timeout(Duration::from_millis(500), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        // 3rd blocked (2 in-flight, none released)
        let blocked = tokio::time::timeout(Duration::from_millis(200), stream.next()).await;
        assert!(blocked.is_err(), "3rd message should be blocked by semaphore");

        // Drop m1 to release a semaphore slot
        drop(m1);

        // Now 3rd should arrive
        let _m3 = tokio::time::timeout(Duration::from_millis(500), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
