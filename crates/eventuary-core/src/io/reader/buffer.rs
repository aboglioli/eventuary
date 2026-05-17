//! BufferedReader: durable at-least-once buffer between an inner
//! reader and downstream handlers.
//!
//! Each event drawn from the inner reader is persisted to a
//! `BufferStore` and the inner acker is invoked immediately. The
//! emitted message carries a `BufferAcker` tied to the buffer entry;
//! downstream `ack`/`nack` removes/keeps the entry in the store. On
//! `read`, the store is replayed first so unacked entries from a
//! prior session are delivered before live events.
//!
//! Backpressure is governed by `max_pending` via a `tokio::Semaphore`
//! permit per in-flight buffer entry. Permits are released when an
//! acker is dropped/acked/nacked, so an aborted consumer never wedges
//! the intake loop. This replaces an earlier `AtomicUsize`+`Notify`
//! pattern that had a load/notified race.
//!
//! Failure modes affecting delivery semantics:
//! - Store push succeeds, then inner ack fails: entry stays durable
//!   and replays on restart, while the source may also redeliver. At
//!   least-once double-delivery — pair with a dedupe wrapper if the
//!   downstream handler is not idempotent.
//! - Store push fails after the inner reader produced an item: error
//!   propagates to the stream, the inner ack is not invoked, source
//!   redelivers on restart.
//! - `BufferStore::nack` is store-defined; for `InMemoryBufferStore`
//!   it is a no-op (entry remains in `pending`).

use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;

use futures::StreamExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use crate::error::Result;
use crate::event::Event;
use crate::io::stream::SpawnedStream;
use crate::io::{Acker, Message, Reader};

const CHANNEL_BUFFER: usize = 64;

pub struct BufferEntry<C, Id> {
    pub id: Id,
    pub event: Event,
    pub cursor: C,
}

pub trait BufferStore<C>: Clone + Send + Sync + 'static {
    type Id: Clone + Send + Sync;

    fn push(&self, event: &Event, cursor: &C) -> impl Future<Output = Result<Self::Id>> + Send;

    /// Returns a snapshot of entries currently held by the store
    /// without removing them. Re-calling without ack/nack returns the
    /// same set.
    fn pending(&self) -> impl Future<Output = Result<Vec<BufferEntry<C, Self::Id>>>> + Send;

    fn ack(&self, id: &Self::Id) -> impl Future<Output = Result<()>> + Send;

    fn nack(&self, id: &Self::Id) -> impl Future<Output = Result<()>> + Send;
}

#[derive(Clone)]
pub struct InMemoryBufferStore<C> {
    state: Arc<Mutex<InMemoryState<C>>>,
}

pub struct BufferedReaderConfig {
    pub max_pending: usize,
}

impl Default for BufferedReaderConfig {
    fn default() -> Self {
        Self { max_pending: 1024 }
    }
}

struct InMemoryState<C> {
    entries: HashMap<InMemoryBufferStoreId, InMemoryEntry<C>>,
    next_id: InMemoryBufferStoreId,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct InMemoryBufferStoreId(u64);

struct InMemoryEntry<C> {
    event: Event,
    cursor: C,
}

impl<C> InMemoryBufferStore<C>
where
    C: Clone + Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(InMemoryState {
                entries: HashMap::new(),
                next_id: InMemoryBufferStoreId(0),
            })),
        }
    }

    pub fn pending_count(&self) -> usize {
        self.state.lock().unwrap().entries.len()
    }
}

impl<C> Default for InMemoryBufferStore<C>
where
    C: Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<C> BufferStore<C> for InMemoryBufferStore<C>
where
    C: Clone + Send + Sync + 'static,
{
    type Id = InMemoryBufferStoreId;

    async fn push(&self, event: &Event, cursor: &C) -> Result<Self::Id> {
        let mut state = self.state.lock().unwrap();
        let id = state.next_id;
        state.next_id = InMemoryBufferStoreId(
            id.0.checked_add(1)
                .expect("buffer store id space exhausted"),
        );
        state.entries.insert(
            id,
            InMemoryEntry {
                event: event.clone(),
                cursor: cursor.clone(),
            },
        );
        Ok(id)
    }

    async fn pending(&self) -> Result<Vec<BufferEntry<C, Self::Id>>> {
        let state = self.state.lock().unwrap();
        let entries: Vec<BufferEntry<C, Self::Id>> = state
            .entries
            .iter()
            .map(|(id, e)| BufferEntry {
                id: *id,
                event: e.event.clone(),
                cursor: e.cursor.clone(),
            })
            .collect();
        Ok(entries)
    }

    async fn ack(&self, id: &Self::Id) -> Result<()> {
        self.state.lock().unwrap().entries.remove(id);
        Ok(())
    }

    async fn nack(&self, _id: &Self::Id) -> Result<()> {
        Ok(())
    }
}

pub struct BufferAcker<S: BufferStore<C>, C> {
    store: S,
    id: S::Id,
    permit: Arc<Mutex<Option<OwnedSemaphorePermit>>>,
    _cursor: PhantomData<C>,
}

impl<S, C> BufferAcker<S, C>
where
    S: BufferStore<C>,
{
    fn new(store: S, id: S::Id, permit: OwnedSemaphorePermit) -> Self {
        Self {
            store,
            id,
            permit: Arc::new(Mutex::new(Some(permit))),
            _cursor: PhantomData,
        }
    }

    fn release_slot(&self) {
        self.permit.lock().unwrap().take();
    }
}

impl<S, C> Acker for BufferAcker<S, C>
where
    S: BufferStore<C> + 'static,
    C: Send + Sync + 'static,
{
    async fn ack(&self) -> Result<()> {
        self.store.ack(&self.id).await?;
        self.release_slot();
        Ok(())
    }

    async fn nack(&self) -> Result<()> {
        self.store.nack(&self.id).await?;
        self.release_slot();
        Ok(())
    }
}

impl<S, C> Clone for BufferAcker<S, C>
where
    S: BufferStore<C> + Clone,
{
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            id: self.id.clone(),
            permit: Arc::clone(&self.permit),
            _cursor: PhantomData,
        }
    }
}

impl<S, C> Drop for BufferAcker<S, C>
where
    S: BufferStore<C>,
{
    fn drop(&mut self) {
        self.release_slot();
    }
}

pub struct BufferedReader<R, S> {
    inner: R,
    store: S,
    config: BufferedReaderConfig,
}

impl<R, S> BufferedReader<R, S> {
    pub fn new(inner: R, store: S) -> Self {
        Self {
            inner,
            store,
            config: BufferedReaderConfig::default(),
        }
    }

    pub fn with_config(inner: R, store: S, config: BufferedReaderConfig) -> Self {
        Self {
            inner,
            store,
            config,
        }
    }
}

impl<R, S> Reader for BufferedReader<R, S>
where
    R: Reader + Send + Sync + 'static,
    R::Cursor: Clone + Send + Sync + 'static,
    R::Subscription: Send + 'static,
    R::Acker: Acker + 'static,
    R::Stream: Send + 'static,
    S: BufferStore<R::Cursor> + 'static,
{
    type Subscription = R::Subscription;
    type Acker = BufferAcker<S, R::Cursor>;
    type Cursor = R::Cursor;
    type Stream = SpawnedStream<BufferAcker<S, R::Cursor>, R::Cursor>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let store = self.store.clone();
        let (tx, rx) =
            mpsc::channel::<Result<Message<BufferAcker<S, R::Cursor>, R::Cursor>>>(CHANNEL_BUFFER);

        let pending_entries = store.pending().await?;
        let inner = self.inner.read(subscription).await?;
        let semaphore = Arc::new(Semaphore::new(self.config.max_pending));

        let handle = tokio::spawn(async move {
            let mut inner = Box::pin(inner);

            for entry in pending_entries {
                let permit = match Arc::clone(&semaphore).acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => return,
                };
                let acker = BufferAcker::new(store.clone(), entry.id, permit);
                let msg = Message::new(entry.event, acker, entry.cursor);
                if tx.send(Ok(msg)).await.is_err() {
                    return;
                }
            }

            loop {
                let permit = match Arc::clone(&semaphore).acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => return,
                };

                let item = inner.next().await;
                let msg = match item {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                    None => return,
                };

                let id = match store.push(msg.event(), msg.cursor()).await {
                    Ok(id) => id,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };

                let (event, inner_acker, cursor) = msg.into_parts();

                if let Err(e) = inner_acker.ack().await {
                    let _ = tx
                        .send(Err(crate::error::Error::Store(format!(
                            "buffer reader: inner ack failed: {e}"
                        ))))
                        .await;
                    return;
                }

                let acker = BufferAcker::new(store.clone(), id, permit);
                let out = Message::new(event, acker, cursor);

                if tx.send(Ok(out)).await.is_err() {
                    return;
                }
            }
        });

        Ok(SpawnedStream::new(rx, handle))
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use futures::{Stream, StreamExt, stream};

    use super::*;
    use crate::io::acker::NoopAcker;
    use crate::io::{Message, Reader};
    use crate::payload::Payload;

    fn ev(key: &str) -> Event {
        Event::builder("acme", "/x", "thing.happened", Payload::from_string("p"))
            .unwrap()
            .key(key)
            .unwrap()
            .build()
            .expect("valid event")
    }

    fn ev_basic() -> Event {
        Event::create("org", "/x", "thing.happened", Payload::from_string("p")).unwrap()
    }

    #[tokio::test]
    async fn store_push_returns_incrementing_ids() {
        let store = InMemoryBufferStore::<()>::new();
        let id1 = store.push(&ev_basic(), &()).await.unwrap();
        let id2 = store.push(&ev_basic(), &()).await.unwrap();
        assert!(id1.0 < id2.0);
    }

    #[tokio::test]
    async fn pending_returns_persisted_entries() {
        let store = InMemoryBufferStore::<()>::new();
        store.push(&ev_basic(), &()).await.unwrap();
        let entries = store.pending().await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id.0, 0);
    }

    #[tokio::test]
    async fn ack_removes_from_pending() {
        let store = InMemoryBufferStore::<()>::new();
        let id = store.push(&ev_basic(), &()).await.unwrap();
        store.ack(&id).await.unwrap();
        let entries = store.pending().await.unwrap();
        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn nack_keeps_entry_pending() {
        let store = InMemoryBufferStore::<()>::new();
        let id = store.push(&ev_basic(), &()).await.unwrap();
        store.nack(&id).await.unwrap();
        let entries = store.pending().await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, id);
    }

    #[tokio::test]
    async fn pending_is_idempotent_snapshot() {
        let store = InMemoryBufferStore::<()>::new();
        store.push(&ev_basic(), &()).await.unwrap();
        let first = store.pending().await.unwrap();
        let second = store.pending().await.unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
    }

    #[tokio::test]
    async fn pending_count_reflects_drainable_entries() {
        let store = InMemoryBufferStore::<()>::new();
        assert_eq!(store.pending_count(), 0);
        let id = store.push(&ev_basic(), &()).await.unwrap();
        assert_eq!(store.pending_count(), 1);
        store.ack(&id).await.unwrap();
        assert_eq!(store.pending_count(), 0);
    }

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
            let iter = events
                .into_iter()
                .enumerate()
                .map(|(i, e)| Ok(Message::new(e, NoopAcker, TestCursor(i as u64 + 1))));
            Ok(Box::pin(stream::iter(iter)))
        }
    }

    #[tokio::test]
    async fn delivers_events_and_acks_store() {
        let events: Vec<Event> = (0..3).map(|i| ev(&format!("k{i}"))).collect();
        let store = InMemoryBufferStore::<TestCursor>::new();
        let reader = VecReader {
            events: Mutex::new(Some(events)),
        };
        let buffered = BufferedReader::new(reader, store.clone());
        let mut stream = buffered.read(()).await.unwrap();

        for i in 0..3 {
            let msg = tokio::time::timeout(Duration::from_secs(2), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(msg.event().key().unwrap().as_str(), &format!("k{i}"));
            msg.ack().await.unwrap();
        }

        assert_eq!(store.pending_count(), 0);
    }

    #[tokio::test]
    async fn drain_on_restart_replays_unacked_events() {
        let events: Vec<Event> = (0..3).map(|i| ev(&format!("k{i}"))).collect();
        let store = InMemoryBufferStore::<TestCursor>::new();
        let reader = VecReader {
            events: Mutex::new(Some(events)),
        };
        let buffered = BufferedReader::new(reader, store.clone());
        let mut stream = buffered.read(()).await.unwrap();

        let msg0 = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        msg0.ack().await.unwrap();

        let msg1 = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        msg1.nack().await.unwrap();

        let msg2 = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        drop(msg2);

        assert!(stream.next().await.is_none());

        let store2 = store.clone();
        let reader2 = VecReader {
            events: Mutex::new(Some(vec![])),
        };
        let buffered2 = BufferedReader::new(reader2, store2);
        let mut stream2 = buffered2.read(()).await.unwrap();

        let replayed1 = tokio::time::timeout(Duration::from_secs(2), stream2.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(replayed1.event().key().unwrap().as_str(), "k1");
        replayed1.ack().await.unwrap();

        let replayed2 = tokio::time::timeout(Duration::from_secs(2), stream2.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(replayed2.event().key().unwrap().as_str(), "k2");
        replayed2.ack().await.unwrap();

        assert_eq!(store.pending_count(), 0);
    }

    #[tokio::test]
    async fn inner_acker_called_after_persist() {
        #[derive(Clone, Default)]
        struct CountingAcker {
            count: Arc<AtomicUsize>,
        }

        impl Acker for CountingAcker {
            async fn ack(&self) -> Result<()> {
                self.count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            async fn nack(&self) -> Result<()> {
                Ok(())
            }
        }

        struct CountingReader {
            events: Mutex<Option<Vec<Event>>>,
            acker: CountingAcker,
        }

        impl Reader for CountingReader {
            type Subscription = ();
            type Acker = CountingAcker;
            type Cursor = TestCursor;
            type Stream =
                Pin<Box<dyn Stream<Item = Result<Message<CountingAcker, TestCursor>>> + Send>>;

            async fn read(&self, _: ()) -> Result<Self::Stream> {
                let events = self.events.lock().unwrap().take().unwrap_or_default();
                let acker = self.acker.clone();
                let iter = events.into_iter().enumerate().map(move |(i, e)| {
                    Ok(Message::new(e, acker.clone(), TestCursor(i as u64 + 1)))
                });
                Ok(Box::pin(stream::iter(iter)))
            }
        }

        let acker = CountingAcker::default();
        let store = InMemoryBufferStore::<TestCursor>::new();
        let reader = CountingReader {
            events: Mutex::new(Some(vec![ev("k0")])),
            acker: acker.clone(),
        };
        let buffered = BufferedReader::new(reader, store.clone());
        let mut stream = buffered.read(()).await.unwrap();

        let msg = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(acker.count.load(Ordering::SeqCst), 1);
        assert_eq!(store.pending_count(), 1);

        msg.ack().await.unwrap();
        assert_eq!(store.pending_count(), 0);
    }

    #[tokio::test]
    async fn inner_read_error_propagates() {
        struct FailingReader;

        impl Reader for FailingReader {
            type Subscription = ();
            type Acker = NoopAcker;
            type Cursor = TestCursor;
            type Stream =
                Pin<Box<dyn Stream<Item = Result<Message<NoopAcker, TestCursor>>> + Send>>;

            async fn read(&self, _: ()) -> Result<Self::Stream> {
                Err(crate::error::Error::Store("read failed".into()))
            }
        }

        let store = InMemoryBufferStore::<TestCursor>::new();
        let reader = FailingReader;
        let buffered = BufferedReader::new(reader, store);
        let result = buffered.read(()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn backpressure_blocks_intake_when_buffer_full() {
        let events: Vec<Event> = (0..5).map(|i| ev(&format!("k{i}"))).collect();
        let store = InMemoryBufferStore::<TestCursor>::new();
        let reader = VecReader {
            events: Mutex::new(Some(events)),
        };
        let buffered =
            BufferedReader::with_config(reader, store, BufferedReaderConfig { max_pending: 2 });
        let mut stream = buffered.read(()).await.unwrap();

        let msg0 = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        let _msg1 = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        let blocked = tokio::time::timeout(Duration::from_millis(200), stream.next()).await;
        assert!(
            blocked.is_err(),
            "3rd message should be blocked by max_pending=2"
        );

        msg0.ack().await.unwrap();

        let msg2 = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(msg2.event().key().unwrap().as_str(), "k2");
    }
}
