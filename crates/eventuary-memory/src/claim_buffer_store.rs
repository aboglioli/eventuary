use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::Mutex;

use eventuary_core::Event;
use eventuary_core::Result;
use eventuary_core::io::OwnerId;
use eventuary_core::io::reader::claim_buffer::{ClaimedBufferEntry, ClaimedBufferStore};

#[derive(Debug)]
struct Entry {
    event: Event,
    claimed_by: Option<OwnerId>,
    claimed_until: Option<DateTime<Utc>>,
    attempts: u32,
}

#[derive(Debug, Default)]
struct State {
    next_id: u64,
    entries: BTreeMap<u64, Entry>,
}

#[derive(Debug, Clone, Default)]
pub struct MemoryClaimedBufferStore {
    state: Arc<Mutex<State>>,
}

impl MemoryClaimedBufferStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ClaimedBufferStore for MemoryClaimedBufferStore {
    type Id = u64;

    async fn push(&self, event: &Event) -> Result<Self::Id> {
        let mut state = self.state.lock().await;
        let id = state.next_id;
        state.next_id = id
            .checked_add(1)
            .expect("claimed buffer store id space exhausted");
        state.entries.insert(
            id,
            Entry {
                event: event.clone(),
                claimed_by: None,
                claimed_until: None,
                attempts: 0,
            },
        );
        Ok(id)
    }

    async fn claim_batch(
        &self,
        owner_id: &OwnerId,
        max: usize,
        visibility: Duration,
    ) -> Result<Vec<ClaimedBufferEntry<Self::Id>>> {
        let mut state = self.state.lock().await;
        let now = Utc::now();
        let until =
            now + chrono::Duration::from_std(visibility).unwrap_or(chrono::Duration::MAX);
        let mut claimed = Vec::new();

        for (&id, entry) in state.entries.iter_mut() {
            if claimed.len() >= max {
                break;
            }
            let available = entry.claimed_until.map(|t| t <= now).unwrap_or(true);
            if !available {
                continue;
            }
            entry.claimed_by = Some(owner_id.clone());
            entry.claimed_until = Some(until);
            entry.attempts += 1;
            claimed.push(ClaimedBufferEntry {
                id,
                event: entry.event.clone(),
                attempts: entry.attempts,
            });
        }

        Ok(claimed)
    }

    async fn ack(&self, id: &Self::Id) -> Result<()> {
        self.state.lock().await.entries.remove(id);
        Ok(())
    }

    async fn nack(&self, id: &Self::Id) -> Result<()> {
        let mut state = self.state.lock().await;
        if let Some(entry) = state.entries.get_mut(id) {
            entry.claimed_by = None;
            entry.claimed_until = None;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use eventuary_core::Payload;
    use eventuary_core::io::OwnerId;

    use super::*;

    fn ev(key: &str) -> Event {
        Event::builder("org", "/x", "thing.happened", Payload::from_string("p"))
            .unwrap()
            .key(key)
            .unwrap()
            .build()
            .unwrap()
    }

    fn owner(s: &str) -> OwnerId {
        OwnerId::new(s).unwrap()
    }

    #[tokio::test]
    async fn push_then_claim_returns_entry() {
        let store = MemoryClaimedBufferStore::new();
        store.push(&ev("a")).await.unwrap();
        let batch = store
            .claim_batch(&owner("owner-a"), 10, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].attempts, 1);
    }

    #[tokio::test]
    async fn claim_returns_at_most_max() {
        let store = MemoryClaimedBufferStore::new();
        for i in 0..5 {
            store.push(&ev(&format!("k{i}"))).await.unwrap();
        }
        let first = store
            .claim_batch(&owner("owner-a"), 3, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(first.len(), 3);

        let second = store
            .claim_batch(&owner("owner-b"), 10, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(second.len(), 2);
    }

    #[tokio::test]
    async fn claim_skips_currently_claimed() {
        let store = MemoryClaimedBufferStore::new();
        for i in 0..3 {
            store.push(&ev(&format!("k{i}"))).await.unwrap();
        }
        let first = store
            .claim_batch(&owner("owner-a"), 10, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(first.len(), 3);

        let second = store
            .claim_batch(&owner("owner-b"), 10, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(second.len(), 0);
    }

    #[tokio::test]
    async fn claim_returns_expired_entries() {
        let store = MemoryClaimedBufferStore::new();
        store.push(&ev("a")).await.unwrap();
        store
            .claim_batch(&owner("owner-a"), 10, Duration::from_millis(1))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(5)).await;

        let second = store
            .claim_batch(&owner("owner-b"), 10, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].attempts, 2);
    }

    #[tokio::test]
    async fn ack_removes_entry() {
        let store = MemoryClaimedBufferStore::new();
        store.push(&ev("a")).await.unwrap();
        let batch = store
            .claim_batch(&owner("owner-a"), 10, Duration::from_secs(60))
            .await
            .unwrap();
        store.ack(&batch[0].id).await.unwrap();

        let after = store
            .claim_batch(&owner("owner-b"), 10, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(after.len(), 0);
    }

    #[tokio::test]
    async fn nack_releases_immediately() {
        let store = MemoryClaimedBufferStore::new();
        store.push(&ev("a")).await.unwrap();
        let batch = store
            .claim_batch(&owner("owner-a"), 10, Duration::from_secs(60))
            .await
            .unwrap();
        store.nack(&batch[0].id).await.unwrap();

        let second = store
            .claim_batch(&owner("owner-b"), 10, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].attempts, 2);
    }

    #[tokio::test]
    async fn ack_missing_id_is_noop() {
        let store = MemoryClaimedBufferStore::new();
        let result = store.ack(&9999u64).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn nack_missing_id_is_noop() {
        let store = MemoryClaimedBufferStore::new();
        let result = store.nack(&9999u64).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn fifo_order_preserved() {
        let store = MemoryClaimedBufferStore::new();
        store.push(&ev("first")).await.unwrap();
        store.push(&ev("second")).await.unwrap();
        store.push(&ev("third")).await.unwrap();

        let batch = store
            .claim_batch(&owner("owner-a"), 10, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(batch.len(), 3);
        assert_eq!(batch[0].event.key().unwrap().as_str(), "first");
        assert_eq!(batch[1].event.key().unwrap().as_str(), "second");
        assert_eq!(batch[2].event.key().unwrap().as_str(), "third");
    }
}
