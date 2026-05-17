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
    type Acker = DedupeAcker<R::Acker, S>;
    type Cursor = R::Cursor;
    type Stream = SpawnedStream<DedupeAcker<R::Acker, S>, R::Cursor>;

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
                let event_id: Arc<str> = Arc::from(msg.event().id().to_string());
                match store.exists(&event_id).await {
                    Ok(true) => {
                        if let Err(e) = msg.ack().await {
                            let _ = tx.send(Err(e)).await;
                            return;
                        }
                        continue;
                    }
                    Ok(false) => {}
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                }
                let (event, inner_acker, cursor) = msg.into_parts();
                let wrapped = Message::new(
                    event,
                    DedupeAcker {
                        inner: inner_acker,
                        store: store.clone(),
                        event_id,
                    },
                    cursor,
                );
                if tx.send(Ok(wrapped)).await.is_err() {
                    return;
                }
            }
        });

        Ok(SpawnedStream::new(rx, handle))
    }
}

pub struct DedupeAcker<A: Acker, S: DedupeStore> {
    inner: A,
    store: S,
    event_id: Arc<str>,
}

impl<A: Acker, S: DedupeStore> Acker for DedupeAcker<A, S> {
    async fn ack(&self) -> Result<()> {
        self.store.mark_processed(&self.event_id).await?;
        self.inner.ack().await
    }

    async fn nack(&self) -> Result<()> {
        self.inner.nack().await
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
    use crate::io::Message;
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
            Ok(Box::pin(stream::iter(events.into_iter().enumerate().map(
                |(i, e)| Ok(Message::new(e, NoopAcker, TestCursor(i as u64))),
            ))))
        }
    }

    #[tokio::test]
    async fn nack_before_mark_allows_redelivery() {
        let e = Event::create("org", "/x", "t", Payload::from_string("p")).unwrap();
        let store = InMemoryDedupeStore::new();
        let reader = VecReader {
            events: Mutex::new(Some(vec![e.clone()])),
        };
        let dedup = DedupeReader::new(reader, store.clone());
        let mut stream = dedup.read(()).await.unwrap();

        let m = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        m.nack().await.unwrap();
        assert!(store.seen.lock().unwrap().is_empty());

        let reader = VecReader {
            events: Mutex::new(Some(vec![e])),
        };
        let dedup = DedupeReader::new(reader, store.clone());
        let mut stream = dedup.read(()).await.unwrap();
        let m = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        m.ack().await.unwrap();
        assert_eq!(store.seen.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn dedup_skips_already_marked_events() {
        let e1 = Event::create("org", "/x", "t1", Payload::from_string("p")).unwrap();
        let e2 = Event::create("org", "/x", "t2", Payload::from_string("p")).unwrap();
        let store = InMemoryDedupeStore::new();
        store.mark_processed(&e1.id().to_string()).await.unwrap();

        let reader = VecReader {
            events: Mutex::new(Some(vec![e1, e2.clone()])),
        };
        let dedup = DedupeReader::new(reader, store);
        let mut stream = dedup.read(()).await.unwrap();

        let m = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(m.event().id(), e2.id());
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
