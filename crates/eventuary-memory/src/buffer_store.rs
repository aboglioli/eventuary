//! In-memory [`BufferStore`] implementation.
//!
//! Holds buffered entries in a `HashMap` keyed by a monotonically
//! increasing id. Entries persist until acked; nack is a no-op so the
//! entry remains in the pending snapshot. Suitable for development,
//! tests, and single-process use. For durable buffering across
//! restarts, use a backend-backed store.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use eventuary_core::io::reader::{BufferEntry, BufferStore};
use eventuary_core::{Event, Payload, Result};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct MemoryBufferStoreId(u64);

struct MemoryEntry<C, P> {
    event: Event<P>,
    cursor: C,
}

struct MemoryState<C, P> {
    entries: HashMap<MemoryBufferStoreId, MemoryEntry<C, P>>,
    next_id: MemoryBufferStoreId,
}

pub struct MemoryBufferStore<C, P = Payload> {
    state: Arc<Mutex<MemoryState<C, P>>>,
}

impl<C, P> Clone for MemoryBufferStore<C, P> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl<C, P> MemoryBufferStore<C, P>
where
    C: Clone + Send + Sync + 'static,
    P: Clone + Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(MemoryState {
                entries: HashMap::new(),
                next_id: MemoryBufferStoreId(0),
            })),
        }
    }

    pub fn pending_count(&self) -> usize {
        self.state.lock().unwrap().entries.len()
    }
}

impl<C, P> Default for MemoryBufferStore<C, P>
where
    C: Clone + Send + Sync + 'static,
    P: Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<C, P> BufferStore<C, P> for MemoryBufferStore<C, P>
where
    C: Clone + Send + Sync + 'static,
    P: Clone + Send + Sync + 'static,
{
    type Id = MemoryBufferStoreId;

    async fn push(&self, event: &Event<P>, cursor: &C) -> Result<Self::Id> {
        let mut state = self.state.lock().unwrap();
        let id = state.next_id;
        state.next_id = MemoryBufferStoreId(
            id.0.checked_add(1)
                .expect("buffer store id space exhausted"),
        );
        state.entries.insert(
            id,
            MemoryEntry {
                event: event.clone(),
                cursor: cursor.clone(),
            },
        );
        Ok(id)
    }

    async fn pending(&self) -> Result<Vec<BufferEntry<C, Self::Id, P>>> {
        let state = self.state.lock().unwrap();
        let mut entries: Vec<BufferEntry<C, Self::Id, P>> = state
            .entries
            .iter()
            .map(|(id, e)| BufferEntry {
                id: *id,
                event: e.event.clone(),
                cursor: e.cursor.clone(),
            })
            .collect();
        entries.sort_by_key(|e| e.id.0);
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

#[cfg(test)]
mod tests {
    use super::*;

    use eventuary_core::Payload;

    fn ev() -> Event {
        Event::create("org", "/x", "thing.happened", Payload::from_string("p")).unwrap()
    }

    #[tokio::test]
    async fn push_returns_incrementing_ids() {
        let store = MemoryBufferStore::<()>::new();
        let id1 = store.push(&ev(), &()).await.unwrap();
        let id2 = store.push(&ev(), &()).await.unwrap();
        assert!(id1.0 < id2.0);
    }

    #[tokio::test]
    async fn pending_returns_persisted_entries() {
        let store = MemoryBufferStore::<()>::new();
        store.push(&ev(), &()).await.unwrap();
        let entries = store.pending().await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id.0, 0);
    }

    #[tokio::test]
    async fn ack_removes_from_pending() {
        let store = MemoryBufferStore::<()>::new();
        let id = store.push(&ev(), &()).await.unwrap();
        store.ack(&id).await.unwrap();
        let entries = store.pending().await.unwrap();
        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn nack_keeps_entry_pending() {
        let store = MemoryBufferStore::<()>::new();
        let id = store.push(&ev(), &()).await.unwrap();
        store.nack(&id).await.unwrap();
        let entries = store.pending().await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, id);
    }

    #[tokio::test]
    async fn pending_is_idempotent_snapshot() {
        let store = MemoryBufferStore::<()>::new();
        store.push(&ev(), &()).await.unwrap();
        let first = store.pending().await.unwrap();
        let second = store.pending().await.unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
    }

    #[tokio::test]
    async fn pending_count_reflects_drainable_entries() {
        let store = MemoryBufferStore::<()>::new();
        assert_eq!(store.pending_count(), 0);
        let id = store.push(&ev(), &()).await.unwrap();
        assert_eq!(store.pending_count(), 1);
        store.ack(&id).await.unwrap();
        assert_eq!(store.pending_count(), 0);
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct OrderPlaced {
        order_id: String,
    }

    #[tokio::test]
    async fn memory_buffer_store_carries_typed_payload_round_trip() {
        let store: MemoryBufferStore<(), OrderPlaced> = MemoryBufferStore::new();
        let event = Event::create(
            "acme",
            "/orders",
            "order.placed",
            OrderPlaced {
                order_id: "o-1".into(),
            },
        )
        .unwrap();

        let id = store.push(&event, &()).await.unwrap();
        let entries = store.pending().await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].event.payload().order_id, "o-1");

        store.ack(&id).await.unwrap();
        assert!(store.pending().await.unwrap().is_empty());
    }
}
