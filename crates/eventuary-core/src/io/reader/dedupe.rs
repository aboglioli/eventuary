use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;

use futures::StreamExt;
use tokio::sync::mpsc;

use crate::error::Result;
use crate::io::stream::SpawnedStream;
use crate::io::{Acker, Message, Reader};

pub trait DedupeStore: Clone + Send + Sync + 'static {
    fn exists(&self, event_id: &str) -> impl Future<Output = Result<bool>> + Send;
    fn mark_processed(&self, event_id: &str) -> impl Future<Output = Result<()>> + Send;
}

pub struct DedupeReader<R, S> {
    inner: R,
    store: S,
}

impl<R, S> DedupeReader<R, S> {
    pub fn new(inner: R, store: S) -> Self {
        Self { inner, store }
    }
}

impl<R, S> Reader for DedupeReader<R, S>
where
    R: Reader + Send + Sync + 'static,
    R::Subscription: Send + 'static,
    R::Acker: Send + Sync + 'static,
    R::Cursor: Send + Sync + 'static,
    R::Stream: Send + 'static,
    S: DedupeStore + 'static,
{
    type Subscription = R::Subscription;
    type Acker = R::Acker;
    type Cursor = R::Cursor;
    type Stream = SpawnedStream<R::Acker, R::Cursor>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let inner = self.inner.read(subscription).await?;
        let store = self.store.clone();
        let (tx, rx) = mpsc::channel(64);

        let handle = tokio::spawn(async move {
            let mut stream = Box::pin(inner);
            while let Some(item) = stream.next().await {
                let msg = match item {
                    Ok(m) => m,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };
                let event_id = msg.event().id().to_string();
                match store.exists(&event_id).await {
                    Ok(true) => {
                        if let Err(e) = msg.ack().await {
                            let _ = tx.send(Err(e)).await;
                        }
                        continue;
                    }
                    Ok(false) => {
                        if let Err(e) = store.mark_processed(&event_id).await {
                            let _ = tx.send(Err(e)).await;
                            return;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                }
                if tx.send(Ok(msg)).await.is_err() {
                    return;
                }
            }
        });

        Ok(SpawnedStream::new(rx, handle))
    }
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryDedupeStore {
    seen: Arc<Mutex<HashSet<String>>>,
}

impl InMemoryDedupeStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl DedupeStore for InMemoryDedupeStore {
    async fn exists(&self, event_id: &str) -> Result<bool> {
        Ok(self.seen.lock().unwrap().contains(event_id))
    }

    async fn mark_processed(&self, event_id: &str) -> Result<()> {
        self.seen.lock().unwrap().insert(event_id.to_owned());
        Ok(())
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

    impl crate::io::Cursor for TestCursor {}

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

    #[tokio::test]
    async fn dedup_skips_duplicate_events() {
        let e = Event::create("org", "/x", "t", Payload::from_string("p")).unwrap();

        let reader = VecReader {
            events: Mutex::new(Some(vec![e.clone(), e])),
        };
        let store = InMemoryDedupeStore::new();
        let dedup = DedupeReader::new(reader, store);
        let mut stream = dedup.read(()).await.unwrap();

        let first = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        // Second event has same id (clone) — should be skipped (stream ends or idles).
        let second = tokio::time::timeout(Duration::from_millis(300), stream.next()).await;
        match second {
            Err(_) => {} // timeout = expected (stream idles)
            Ok(None) => {} // stream ended = expected (inner exhausted)
            Ok(Some(Ok(_))) => panic!("duplicate event should be skipped, got a message"),
            Ok(Some(Err(_))) => panic!("duplicate event should be skipped, got error"),
        }
        drop(first);
    }

    #[tokio::test]
    async fn delivers_two_unique_events() {
        let e1 = Event::create("org", "/x", "t1", Payload::from_string("p")).unwrap();
        let e2 = Event::create("org", "/x", "t2", Payload::from_string("p")).unwrap();

        let reader = VecReader {
            events: Mutex::new(Some(vec![e1, e2])),
        };
        let store = InMemoryDedupeStore::new();
        let dedup = DedupeReader::new(reader, store);
        let mut stream = dedup.read(()).await.unwrap();

        let first = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        first.ack().await.unwrap();

        let second = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        second.ack().await.unwrap();
    }
}
