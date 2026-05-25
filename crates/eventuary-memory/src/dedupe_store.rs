//! In-memory [`DedupeStore`] implementation.
//!
//! Records event ids in a `HashSet` with an optional FIFO
//! insertion-order eviction policy. Suitable for development, tests,
//! and single-process use. For multi-process or durable scenarios,
//! use a backend-backed store.

use std::collections::{HashSet, VecDeque};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;

use eventuary_core::io::reader::DedupeStore;
use eventuary_core::{Event, Result};

#[derive(Debug, Default)]
struct DedupeState {
    set: HashSet<String>,
    order: VecDeque<String>,
}

#[derive(Debug, Clone)]
pub struct MemoryDedupeStore {
    state: Arc<Mutex<DedupeState>>,
    capacity: Option<NonZeroUsize>,
}

impl Default for MemoryDedupeStore {
    fn default() -> Self {
        Self::unbounded()
    }
}

impl MemoryDedupeStore {
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

impl<P: Send + Sync + 'static> DedupeStore<P> for MemoryDedupeStore {
    async fn exists(&self, event: &Event<P>) -> Result<bool> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .set
            .contains(&event.id().to_string()))
    }

    async fn mark_processed(&self, event: &Event<P>) -> Result<()> {
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

    async fn mark_if_new(&self, event: &Event<P>) -> Result<bool> {
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
    use super::*;

    use eventuary_core::Payload;

    fn ev(topic: &str) -> Event {
        Event::create("org", "/x", topic, "entity-1", Payload::from_string("p")).unwrap()
    }

    #[tokio::test]
    async fn marks_and_finds_events() {
        let store = MemoryDedupeStore::unbounded();
        let event = ev("a");
        assert!(!store.exists(&event).await.unwrap());
        store.mark_processed(&event).await.unwrap();
        assert!(store.exists(&event).await.unwrap());
    }

    #[tokio::test]
    async fn mark_if_new_returns_false_on_duplicate() {
        let store = MemoryDedupeStore::unbounded();
        let event = ev("a");
        assert!(store.mark_if_new(&event).await.unwrap());
        assert!(!store.mark_if_new(&event).await.unwrap());
    }

    #[tokio::test]
    async fn evicts_oldest_when_capacity_exceeded() {
        let store = MemoryDedupeStore::with_capacity(NonZeroUsize::new(2).unwrap());
        let e1 = ev("a");
        let e2 = ev("b");
        let e3 = ev("c");
        store.mark_processed(&e1).await.unwrap();
        store.mark_processed(&e2).await.unwrap();
        store.mark_processed(&e3).await.unwrap();

        assert_eq!(store.len(), 2);
        assert!(!store.exists(&e1).await.unwrap());
        assert!(store.exists(&e2).await.unwrap());
        assert!(store.exists(&e3).await.unwrap());
    }

    #[tokio::test]
    async fn clear_resets_state() {
        let store = MemoryDedupeStore::unbounded();
        store.mark_processed(&ev("a")).await.unwrap();
        assert!(!store.is_empty());
        store.clear();
        assert!(store.is_empty());
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct OrderPlaced {
        order_id: String,
    }

    #[tokio::test]
    async fn memory_dedupe_store_works_for_typed_payloads() {
        let store = MemoryDedupeStore::unbounded();
        let event: Event<OrderPlaced> = Event::create(
            "acme",
            "/orders",
            "order.placed",
            "order-1",
            OrderPlaced {
                order_id: "o-1".into(),
            },
        )
        .unwrap();

        assert!(
            !<MemoryDedupeStore as DedupeStore<OrderPlaced>>::exists(&store, &event)
                .await
                .unwrap()
        );
        assert!(
            <MemoryDedupeStore as DedupeStore<OrderPlaced>>::mark_if_new(&store, &event)
                .await
                .unwrap()
        );
        assert!(
            <MemoryDedupeStore as DedupeStore<OrderPlaced>>::exists(&store, &event)
                .await
                .unwrap()
        );
        assert!(
            !<MemoryDedupeStore as DedupeStore<OrderPlaced>>::mark_if_new(&store, &event)
                .await
                .unwrap()
        );
    }
}
