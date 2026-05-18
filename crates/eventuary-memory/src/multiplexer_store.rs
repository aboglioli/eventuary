//! In-memory [`MultiplexerStore`] implementation.
//!
//! Records `(event_id, subscriber_id)` pairs in a `HashSet` with an
//! optional FIFO insertion-order eviction policy. Suitable for
//! development, tests, and single-process use. For multi-process or
//! durable scenarios, use a backend-backed store.

use std::collections::{HashSet, VecDeque};
use std::num::NonZeroUsize;
use std::sync::Arc;

use tokio::sync::Mutex;

use eventuary_core::Result;
use eventuary_core::io::handler::{MultiplexerKey, MultiplexerStore};

#[derive(Debug)]
struct MemoryState {
    set: HashSet<MultiplexerKey>,
    order: VecDeque<MultiplexerKey>,
}

#[derive(Debug, Clone)]
pub struct MemoryMultiplexerStore {
    state: Arc<Mutex<MemoryState>>,
    capacity: Option<NonZeroUsize>,
}

impl Default for MemoryMultiplexerStore {
    fn default() -> Self {
        Self::unbounded()
    }
}

impl MemoryMultiplexerStore {
    pub fn unbounded() -> Self {
        Self {
            state: Arc::new(Mutex::new(MemoryState {
                set: HashSet::new(),
                order: VecDeque::new(),
            })),
            capacity: None,
        }
    }

    pub fn with_capacity(capacity: NonZeroUsize) -> Self {
        Self {
            state: Arc::new(Mutex::new(MemoryState {
                set: HashSet::new(),
                order: VecDeque::new(),
            })),
            capacity: Some(capacity),
        }
    }

    pub async fn len(&self) -> usize {
        self.state.lock().await.set.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.state.lock().await.set.is_empty()
    }

    pub async fn clear(&self) {
        let mut state = self.state.lock().await;
        state.set.clear();
        state.order.clear();
    }
}

impl MultiplexerStore for MemoryMultiplexerStore {
    async fn is_completed(&self, key: &MultiplexerKey) -> Result<bool> {
        Ok(self.state.lock().await.set.contains(key))
    }

    /// Bounded variant evicts in **FIFO insertion order** when capacity
    /// is exceeded. Re-marking an already-completed key is a no-op and
    /// does not refresh recency.
    async fn mark_completed(&self, key: &MultiplexerKey) -> Result<()> {
        let mut state = self.state.lock().await;
        if state.set.insert(key.clone()) {
            state.order.push_back(key.clone());
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
}

#[cfg(test)]
mod tests {
    use super::*;

    use eventuary_core::EventId;
    use eventuary_core::io::handler::SubscriberId;

    fn key(name: &str) -> MultiplexerKey {
        MultiplexerKey::new(EventId::new(), SubscriberId::new(name).unwrap())
    }

    #[tokio::test]
    async fn records_completion() {
        let store = MemoryMultiplexerStore::unbounded();
        let k = key("audit");
        assert!(!store.is_completed(&k).await.unwrap());
        store.mark_completed(&k).await.unwrap();
        assert!(store.is_completed(&k).await.unwrap());
        assert_eq!(store.len().await, 1);
    }

    #[tokio::test]
    async fn evicts_oldest_when_capacity_exceeded() {
        let store = MemoryMultiplexerStore::with_capacity(NonZeroUsize::new(2).unwrap());
        let k1 = key("a");
        let k2 = key("b");
        let k3 = key("c");
        store.mark_completed(&k1).await.unwrap();
        store.mark_completed(&k2).await.unwrap();
        store.mark_completed(&k3).await.unwrap();

        assert_eq!(store.len().await, 2);
        assert!(!store.is_completed(&k1).await.unwrap());
        assert!(store.is_completed(&k2).await.unwrap());
        assert!(store.is_completed(&k3).await.unwrap());
    }

    #[tokio::test]
    async fn clear_resets_state() {
        let store = MemoryMultiplexerStore::unbounded();
        store.mark_completed(&key("a")).await.unwrap();
        assert!(!store.is_empty().await);
        store.clear().await;
        assert!(store.is_empty().await);
    }

    #[tokio::test]
    async fn re_mark_is_idempotent() {
        let store = MemoryMultiplexerStore::unbounded();
        let k = key("a");
        store.mark_completed(&k).await.unwrap();
        store.mark_completed(&k).await.unwrap();
        assert_eq!(store.len().await, 1);
    }
}
