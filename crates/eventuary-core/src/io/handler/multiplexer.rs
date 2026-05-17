use std::collections::{HashSet, VecDeque};
use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::error::{Error, Result};
use crate::event::EventId;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct SubscriberId(Arc<str>);

impl SubscriberId {
    pub fn new(value: impl AsRef<str>) -> Result<Self> {
        let value = value.as_ref();
        if value.is_empty() || value.len() > 128 {
            return Err(Error::Config(format!(
                "invalid subscriber id length: {value:?}"
            )));
        }
        if !value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.' || c == ':')
        {
            return Err(Error::Config(format!("invalid subscriber id: {value:?}")));
        }
        Ok(Self(Arc::from(value)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SubscriberId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct MultiplexerKey {
    pub event_id: EventId,
    pub subscriber_id: SubscriberId,
}

impl MultiplexerKey {
    pub fn new(event_id: EventId, subscriber_id: SubscriberId) -> Self {
        Self {
            event_id,
            subscriber_id,
        }
    }
}

pub trait MultiplexerStore: Clone + Send + Sync + 'static {
    fn is_completed<'a>(
        &'a self,
        key: &'a MultiplexerKey,
    ) -> impl Future<Output = Result<bool>> + Send + 'a;

    fn mark_completed<'a>(
        &'a self,
        key: &'a MultiplexerKey,
    ) -> impl Future<Output = Result<()>> + Send + 'a;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoMultiplexerStore;

impl MultiplexerStore for NoMultiplexerStore {
    async fn is_completed(&self, _: &MultiplexerKey) -> Result<bool> {
        Ok(false)
    }

    async fn mark_completed(&self, _: &MultiplexerKey) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct InMemoryStoreState {
    set: HashSet<MultiplexerKey>,
    order: VecDeque<MultiplexerKey>,
}

#[derive(Debug, Clone)]
pub struct InMemoryMultiplexerStore {
    state: Arc<Mutex<InMemoryStoreState>>,
    capacity: Option<NonZeroUsize>,
}

impl Default for InMemoryMultiplexerStore {
    fn default() -> Self {
        Self::unbounded()
    }
}

impl InMemoryMultiplexerStore {
    pub fn unbounded() -> Self {
        Self {
            state: Arc::new(Mutex::new(InMemoryStoreState {
                set: HashSet::new(),
                order: VecDeque::new(),
            })),
            capacity: None,
        }
    }

    pub fn with_capacity(capacity: NonZeroUsize) -> Self {
        Self {
            state: Arc::new(Mutex::new(InMemoryStoreState {
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

impl MultiplexerStore for InMemoryMultiplexerStore {
    async fn is_completed(&self, key: &MultiplexerKey) -> Result<bool> {
        Ok(self.state.lock().await.set.contains(key))
    }

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

    use std::num::NonZeroUsize;

    use crate::event::EventId;

    #[test]
    fn subscriber_id_accepts_stable_names() {
        let id = SubscriberId::new("orders-projection.v1:blue").unwrap();
        assert_eq!(id.as_str(), "orders-projection.v1:blue");
    }

    #[test]
    fn subscriber_id_rejects_invalid_values() {
        assert!(SubscriberId::new("").is_err());
        assert!(SubscriberId::new("bad space").is_err());
        assert!(SubscriberId::new("bad/slash").is_err());
        assert!(SubscriberId::new("a".repeat(129)).is_err());
    }

    #[test]
    fn multiplexer_key_exposes_components() {
        let event_id = EventId::new();
        let subscriber_id = SubscriberId::new("audit").unwrap();
        let key = MultiplexerKey::new(event_id, subscriber_id.clone());
        assert_eq!(key.event_id, event_id);
        assert_eq!(key.subscriber_id, subscriber_id);
    }

    #[tokio::test]
    async fn no_multiplexer_store_reports_nothing_completed() {
        let store = NoMultiplexerStore;
        let key = MultiplexerKey::new(EventId::new(), SubscriberId::new("audit").unwrap());
        assert!(!store.is_completed(&key).await.unwrap());
        store.mark_completed(&key).await.unwrap();
        assert!(!store.is_completed(&key).await.unwrap());
    }

    #[tokio::test]
    async fn in_memory_store_records_completion() {
        let store = InMemoryMultiplexerStore::unbounded();
        let key = MultiplexerKey::new(EventId::new(), SubscriberId::new("audit").unwrap());

        assert!(!store.is_completed(&key).await.unwrap());
        store.mark_completed(&key).await.unwrap();
        assert!(store.is_completed(&key).await.unwrap());
        assert_eq!(store.len().await, 1);
    }

    #[tokio::test]
    async fn in_memory_store_evicts_oldest_when_capacity_exceeded() {
        let store = InMemoryMultiplexerStore::with_capacity(NonZeroUsize::new(2).unwrap());
        let k1 = MultiplexerKey::new(EventId::new(), SubscriberId::new("a").unwrap());
        let k2 = MultiplexerKey::new(EventId::new(), SubscriberId::new("b").unwrap());
        let k3 = MultiplexerKey::new(EventId::new(), SubscriberId::new("c").unwrap());
        store.mark_completed(&k1).await.unwrap();
        store.mark_completed(&k2).await.unwrap();
        store.mark_completed(&k3).await.unwrap();

        assert_eq!(store.len().await, 2);
        assert!(!store.is_completed(&k1).await.unwrap());
        assert!(store.is_completed(&k2).await.unwrap());
        assert!(store.is_completed(&k3).await.unwrap());
    }

    #[tokio::test]
    async fn in_memory_store_clear_resets_state() {
        let store = InMemoryMultiplexerStore::unbounded();
        store
            .mark_completed(&MultiplexerKey::new(
                EventId::new(),
                SubscriberId::new("a").unwrap(),
            ))
            .await
            .unwrap();
        assert!(!store.is_empty().await);
        store.clear().await;
        assert!(store.is_empty().await);
    }
}
