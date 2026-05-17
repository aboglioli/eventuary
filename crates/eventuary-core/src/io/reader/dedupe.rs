use std::collections::{HashSet, VecDeque};
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;

use futures::StreamExt;
use tokio::sync::mpsc;

use crate::error::Result;
use crate::event::Event;
use crate::io::stream::SpawnedStream;
use crate::io::{Acker, Message, Reader};

pub trait DedupeStore: Clone + Send + Sync + 'static {
    fn exists(&self, event: &Event) -> impl Future<Output = Result<bool>> + Send;
    fn mark_processed(&self, event: &Event) -> impl Future<Output = Result<()>> + Send;

    /// Atomically marks the event as seen, returning whether it was newly
    /// recorded (`true`) or was already present (`false`). The default
    /// implementation calls `exists` then `mark_processed`, which is racy
    /// across concurrent callers. Backends with native atomicity (e.g.,
    /// Postgres `INSERT ON CONFLICT DO NOTHING`) should override.
    fn mark_if_new(&self, event: &Event) -> impl Future<Output = Result<bool>> + Send {
        async move {
            if self.exists(event).await? {
                return Ok(false);
            }
            self.mark_processed(event).await?;
            Ok(true)
        }
    }
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
                match store.exists(msg.event()).await {
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
                let (event_arc, inner_acker, cursor) = msg.into_parts_arc();
                let wrapped = Message::from_arc(
                    Arc::clone(&event_arc),
                    DedupeAcker {
                        inner: inner_acker,
                        store: store.clone(),
                        event: event_arc,
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
    event: Arc<Event>,
}

impl<A: Acker, S: DedupeStore> Acker for DedupeAcker<A, S> {
    async fn ack(&self) -> Result<()> {
        self.store.mark_processed(&self.event).await?;
        self.inner.ack().await
    }

    async fn nack(&self) -> Result<()> {
        self.inner.nack().await
    }
}

#[derive(Debug, Clone)]
pub struct InMemoryDedupeStore {
    state: Arc<Mutex<DedupeState>>,
    capacity: Option<NonZeroUsize>,
}

#[derive(Debug, Default)]
struct DedupeState {
    set: HashSet<String>,
    order: VecDeque<String>,
}

impl Default for InMemoryDedupeStore {
    fn default() -> Self {
        Self::unbounded()
    }
}

impl InMemoryDedupeStore {
    pub fn unbounded() -> Self {
        Self {
            state: Arc::new(Mutex::new(DedupeState::default())),
            capacity: None,
        }
    }

    pub fn with_capacity(capacity: NonZeroUsize) -> Self {
        Self {
            state: Arc::new(Mutex::new(DedupeState::default())),
            capacity: Some(capacity),
        }
    }

    pub fn len(&self) -> usize {
        self.state.lock().unwrap().set.len()
    }

    pub fn is_empty(&self) -> bool {
        self.state.lock().unwrap().set.is_empty()
    }

    pub fn clear(&self) {
        let mut state = self.state.lock().unwrap();
        state.set.clear();
        state.order.clear();
    }
}

impl DedupeStore for InMemoryDedupeStore {
    async fn exists(&self, event: &Event) -> Result<bool> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .set
            .contains(&event.id().to_string()))
    }

    async fn mark_processed(&self, event: &Event) -> Result<()> {
        let key = event.id().to_string();
        let mut state = self.state.lock().unwrap();
        if state.set.insert(key.clone()) {
            state.order.push_back(key);
            if let Some(cap) = self.capacity {
                while state.set.len() > cap.get() {
                    if let Some(oldest) = state.order.pop_front() {
                        state.set.remove(&oldest);
                    } else {
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    async fn mark_if_new(&self, event: &Event) -> Result<bool> {
        let key = event.id().to_string();
        let mut state = self.state.lock().unwrap();
        if !state.set.insert(key.clone()) {
            return Ok(false);
        }
        state.order.push_back(key);
        if let Some(cap) = self.capacity {
            while state.set.len() > cap.get() {
                if let Some(oldest) = state.order.pop_front() {
                    state.set.remove(&oldest);
                } else {
                    break;
                }
            }
        }
        Ok(true)
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

    fn ev(topic: &str) -> Event {
        Event::create("org", "/x", topic, Payload::from_string("p")).unwrap()
    }

    #[tokio::test]
    async fn nack_before_mark_allows_redelivery() {
        let e = Event::create("org", "/x", "t", Payload::from_string("p")).unwrap();
        let store = InMemoryDedupeStore::unbounded();
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
        assert_eq!(store.len(), 0);

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
        assert_eq!(store.len(), 1);
    }

    #[tokio::test]
    async fn dedup_skips_already_marked_events() {
        let e1 = ev("t1");
        let e2 = ev("t2");
        let store = InMemoryDedupeStore::unbounded();
        store.mark_processed(&e1).await.unwrap();

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
    async fn capped_store_evicts_oldest() {
        let store = InMemoryDedupeStore::with_capacity(NonZeroUsize::new(2).unwrap());
        let e1 = ev("t1");
        let e2 = ev("t2");
        let e3 = ev("t3");
        store.mark_processed(&e1).await.unwrap();
        store.mark_processed(&e2).await.unwrap();
        store.mark_processed(&e3).await.unwrap();
        assert_eq!(store.len(), 2);
        assert!(!store.exists(&e1).await.unwrap());
        assert!(store.exists(&e2).await.unwrap());
        assert!(store.exists(&e3).await.unwrap());
    }

    #[tokio::test]
    async fn mark_if_new_default_returns_correct_status() {
        let store = InMemoryDedupeStore::unbounded();
        let e = ev("t");
        assert!(store.mark_if_new(&e).await.unwrap());
        assert!(!store.mark_if_new(&e).await.unwrap());
    }

    #[tokio::test]
    async fn clear_resets_store() {
        let store = InMemoryDedupeStore::unbounded();
        store.mark_processed(&ev("t1")).await.unwrap();
        store.mark_processed(&ev("t2")).await.unwrap();
        assert_eq!(store.len(), 2);
        store.clear();
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
    }
}
