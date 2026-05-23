//! In-memory [`PartitionCoordinator`] implementation.
//!
//! Holds all coordinator state in a `HashMap` keyed by `(consumer_group_id, stream_id, ...)`.
//! Suitable for development, tests, and single-process use. For durable coordination across
//! restarts and instances, use a backend-backed implementation.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::Mutex;

use eventuary_core::io::{
    ConsumerGroupId, OwnerId, PartitionCoordinator, PartitionLease, StreamId,
};
use eventuary_core::{Error, Result};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct ConsumerKey {
    consumer_group_id: ConsumerGroupId,
    stream_id: StreamId,
    owner_id: OwnerId,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct PartitionKey {
    consumer_group_id: ConsumerGroupId,
    stream_id: StreamId,
    partition_id: u16,
}

#[derive(Debug, Clone)]
struct PartitionState {
    owner_id: Option<OwnerId>,
    lease_until: Option<DateTime<Utc>>,
    generation: i64,
    checkpoint_sequence: i64,
}

#[derive(Debug, Default)]
struct CoordinatorState {
    consumers: HashMap<ConsumerKey, DateTime<Utc>>,
    partitions: HashMap<PartitionKey, PartitionState>,
}

#[derive(Debug, Clone, Default)]
pub struct MemoryPartitionCoordinator {
    state: Arc<Mutex<CoordinatorState>>,
}

impl MemoryPartitionCoordinator {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PartitionCoordinator for MemoryPartitionCoordinator {
    async fn heartbeat<'a>(
        &'a self,
        consumer_group_id: &'a ConsumerGroupId,
        stream_id: &'a StreamId,
        owner_id: &'a OwnerId,
        lease_duration: Duration,
    ) -> Result<()> {
        let key = ConsumerKey {
            consumer_group_id: consumer_group_id.clone(),
            stream_id: stream_id.clone(),
            owner_id: owner_id.clone(),
        };
        let lease_until = Utc::now() + lease_duration;
        self.state.lock().await.consumers.insert(key, lease_until);
        Ok(())
    }

    async fn live_consumers<'a>(
        &'a self,
        consumer_group_id: &'a ConsumerGroupId,
        stream_id: &'a StreamId,
    ) -> Result<usize> {
        let now = Utc::now();
        let state = self.state.lock().await;
        let count = state
            .consumers
            .iter()
            .filter(|(k, lease_until)| {
                &k.consumer_group_id == consumer_group_id
                    && &k.stream_id == stream_id
                    && **lease_until > now
            })
            .count();
        Ok(count)
    }

    async fn claim<'a>(
        &'a self,
        consumer_group_id: &'a ConsumerGroupId,
        stream_id: &'a StreamId,
        partition_id: u16,
        owner_id: &'a OwnerId,
        lease_duration: Duration,
    ) -> Result<Option<PartitionLease>> {
        let key = PartitionKey {
            consumer_group_id: consumer_group_id.clone(),
            stream_id: stream_id.clone(),
            partition_id,
        };
        let now = Utc::now();
        let lease_until = now + lease_duration;
        let mut state = self.state.lock().await;

        match state.partitions.get_mut(&key) {
            None => {
                let entry = PartitionState {
                    owner_id: Some(owner_id.clone()),
                    lease_until: Some(lease_until),
                    generation: 1,
                    checkpoint_sequence: 0,
                };
                let lease = PartitionLease {
                    consumer_group_id: consumer_group_id.clone(),
                    stream_id: stream_id.clone(),
                    partition_id,
                    owner_id: owner_id.clone(),
                    generation: entry.generation,
                    checkpoint_sequence: entry.checkpoint_sequence,
                    lease_until,
                };
                state.partitions.insert(key, entry);
                Ok(Some(lease))
            }
            Some(entry) => {
                let is_expired = entry.lease_until.map(|t| t <= now).unwrap_or(true);
                let is_unowned = entry.owner_id.is_none();
                let is_self = entry.owner_id.as_ref() == Some(owner_id);

                if is_unowned || is_expired || is_self {
                    entry.generation += 1;
                    entry.owner_id = Some(owner_id.clone());
                    entry.lease_until = Some(lease_until);
                    let lease = PartitionLease {
                        consumer_group_id: consumer_group_id.clone(),
                        stream_id: stream_id.clone(),
                        partition_id,
                        owner_id: owner_id.clone(),
                        generation: entry.generation,
                        checkpoint_sequence: entry.checkpoint_sequence,
                        lease_until,
                    };
                    Ok(Some(lease))
                } else {
                    Ok(None)
                }
            }
        }
    }

    async fn renew<'a>(
        &'a self,
        consumer_group_id: &'a ConsumerGroupId,
        stream_id: &'a StreamId,
        partition_id: u16,
        owner_id: &'a OwnerId,
        generation: i64,
        lease_duration: Duration,
    ) -> Result<()> {
        let key = PartitionKey {
            consumer_group_id: consumer_group_id.clone(),
            stream_id: stream_id.clone(),
            partition_id,
        };
        let mut state = self.state.lock().await;
        match state.partitions.get_mut(&key) {
            Some(entry)
                if entry.owner_id.as_ref() == Some(owner_id) && entry.generation == generation =>
            {
                entry.lease_until = Some(Utc::now() + lease_duration);
                Ok(())
            }
            _ => Err(Error::OwnershipLost(format!(
                "partition {partition_id}: ownership lost or generation mismatch for owner {owner_id} gen {generation}"
            ))),
        }
    }

    async fn release<'a>(
        &'a self,
        consumer_group_id: &'a ConsumerGroupId,
        stream_id: &'a StreamId,
        partition_id: u16,
        owner_id: &'a OwnerId,
        generation: i64,
    ) -> Result<()> {
        let key = PartitionKey {
            consumer_group_id: consumer_group_id.clone(),
            stream_id: stream_id.clone(),
            partition_id,
        };
        let mut state = self.state.lock().await;
        match state.partitions.get_mut(&key) {
            Some(entry)
                if entry.owner_id.as_ref() == Some(owner_id) && entry.generation == generation =>
            {
                entry.owner_id = None;
                entry.lease_until = None;
                entry.generation += 1;
                Ok(())
            }
            _ => Err(Error::OwnershipLost(format!(
                "partition {partition_id}: ownership lost or generation mismatch for owner {owner_id} gen {generation}"
            ))),
        }
    }

    /// Advance `checkpoint_sequence` to `sequence` only if `(owner_id, generation)` matches.
    ///
    /// The update is monotonic: if `sequence` is less than or equal to the stored
    /// `checkpoint_sequence`, the call is a silent no-op and returns `Ok(())`.
    async fn checkpoint<'a>(
        &'a self,
        consumer_group_id: &'a ConsumerGroupId,
        stream_id: &'a StreamId,
        partition_id: u16,
        owner_id: &'a OwnerId,
        generation: i64,
        sequence: i64,
    ) -> Result<()> {
        let key = PartitionKey {
            consumer_group_id: consumer_group_id.clone(),
            stream_id: stream_id.clone(),
            partition_id,
        };
        let mut state = self.state.lock().await;
        match state.partitions.get_mut(&key) {
            Some(entry)
                if entry.owner_id.as_ref() == Some(owner_id) && entry.generation == generation =>
            {
                if sequence > entry.checkpoint_sequence {
                    entry.checkpoint_sequence = sequence;
                }
                Ok(())
            }
            _ => Err(Error::OwnershipLost(format!(
                "partition {partition_id}: ownership lost or generation mismatch for owner {owner_id} gen {generation}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::time::sleep;

    fn group() -> ConsumerGroupId {
        ConsumerGroupId::new("orders-projection-v1").unwrap()
    }

    fn stream() -> StreamId {
        StreamId::new("orders-events").unwrap()
    }

    fn owner_a() -> OwnerId {
        OwnerId::new("instance-a").unwrap()
    }

    fn owner_b() -> OwnerId {
        OwnerId::new("instance-b").unwrap()
    }

    fn long_lease() -> Duration {
        Duration::from_secs(60)
    }

    fn short_lease() -> Duration {
        Duration::from_millis(1)
    }

    #[tokio::test]
    async fn heartbeat_makes_consumer_visible() {
        let coord = MemoryPartitionCoordinator::new();
        coord
            .heartbeat(&group(), &stream(), &owner_a(), long_lease())
            .await
            .unwrap();
        assert_eq!(coord.live_consumers(&group(), &stream()).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn live_consumers_excludes_expired() {
        let coord = MemoryPartitionCoordinator::new();
        coord
            .heartbeat(&group(), &stream(), &owner_a(), short_lease())
            .await
            .unwrap();
        sleep(Duration::from_millis(5)).await;
        assert_eq!(coord.live_consumers(&group(), &stream()).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn claim_free_partition_succeeds() {
        let coord = MemoryPartitionCoordinator::new();
        let lease = coord
            .claim(&group(), &stream(), 0, &owner_a(), long_lease())
            .await
            .unwrap();
        let lease = lease.expect("expected Some(lease)");
        assert_eq!(lease.generation, 1);
        assert_eq!(lease.checkpoint_sequence, 0);
        assert_eq!(lease.owner_id, owner_a());
    }

    #[tokio::test]
    async fn claim_contested_returns_none() {
        let coord = MemoryPartitionCoordinator::new();
        coord
            .claim(&group(), &stream(), 7, &owner_a(), long_lease())
            .await
            .unwrap();
        let result = coord
            .claim(&group(), &stream(), 7, &owner_b(), long_lease())
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn claim_after_expiry_succeeds() {
        let coord = MemoryPartitionCoordinator::new();
        coord
            .claim(&group(), &stream(), 0, &owner_a(), short_lease())
            .await
            .unwrap();
        sleep(Duration::from_millis(5)).await;
        let lease = coord
            .claim(&group(), &stream(), 0, &owner_b(), long_lease())
            .await
            .unwrap();
        let lease = lease.expect("expected Some(lease) after expiry");
        assert_eq!(lease.generation, 2);
        assert_eq!(lease.owner_id, owner_b());
    }

    #[tokio::test]
    async fn renew_with_matching_generation() {
        let coord = MemoryPartitionCoordinator::new();
        let lease = coord
            .claim(&group(), &stream(), 0, &owner_a(), long_lease())
            .await
            .unwrap()
            .unwrap();
        coord
            .renew(
                &group(),
                &stream(),
                0,
                &owner_a(),
                lease.generation,
                long_lease(),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn renew_with_stale_generation_returns_ownership_lost() {
        let coord = MemoryPartitionCoordinator::new();
        let lease_a = coord
            .claim(&group(), &stream(), 0, &owner_a(), short_lease())
            .await
            .unwrap()
            .unwrap();
        sleep(Duration::from_millis(5)).await;
        coord
            .claim(&group(), &stream(), 0, &owner_b(), long_lease())
            .await
            .unwrap();
        let result = coord
            .renew(
                &group(),
                &stream(),
                0,
                &owner_a(),
                lease_a.generation,
                long_lease(),
            )
            .await;
        assert!(matches!(result, Err(Error::OwnershipLost(_))));
    }

    #[tokio::test]
    async fn release_with_matching_generation() {
        let coord = MemoryPartitionCoordinator::new();
        let lease = coord
            .claim(&group(), &stream(), 0, &owner_a(), long_lease())
            .await
            .unwrap()
            .unwrap();
        coord
            .release(&group(), &stream(), 0, &owner_a(), lease.generation)
            .await
            .unwrap();
        let new_lease = coord
            .claim(&group(), &stream(), 0, &owner_b(), long_lease())
            .await
            .unwrap();
        let new_lease = new_lease.expect("expected claim to succeed after release");
        assert_eq!(new_lease.generation, lease.generation + 2);
    }

    #[tokio::test]
    async fn release_with_stale_generation_returns_ownership_lost() {
        let coord = MemoryPartitionCoordinator::new();
        let lease_a = coord
            .claim(&group(), &stream(), 0, &owner_a(), short_lease())
            .await
            .unwrap()
            .unwrap();
        sleep(Duration::from_millis(5)).await;
        coord
            .claim(&group(), &stream(), 0, &owner_b(), long_lease())
            .await
            .unwrap();
        let result = coord
            .release(&group(), &stream(), 0, &owner_a(), lease_a.generation)
            .await;
        assert!(matches!(result, Err(Error::OwnershipLost(_))));
    }

    #[tokio::test]
    async fn checkpoint_with_matching_generation_advances() {
        let coord = MemoryPartitionCoordinator::new();
        let lease = coord
            .claim(&group(), &stream(), 0, &owner_a(), short_lease())
            .await
            .unwrap()
            .unwrap();
        coord
            .checkpoint(&group(), &stream(), 0, &owner_a(), lease.generation, 100)
            .await
            .unwrap();
        sleep(Duration::from_millis(5)).await;
        let new_lease = coord
            .claim(&group(), &stream(), 0, &owner_b(), long_lease())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(new_lease.checkpoint_sequence, 100);
    }

    #[tokio::test]
    async fn checkpoint_with_stale_generation_returns_ownership_lost() {
        let coord = MemoryPartitionCoordinator::new();
        let lease_a = coord
            .claim(&group(), &stream(), 0, &owner_a(), short_lease())
            .await
            .unwrap()
            .unwrap();
        sleep(Duration::from_millis(5)).await;
        coord
            .claim(&group(), &stream(), 0, &owner_b(), long_lease())
            .await
            .unwrap();
        let result = coord
            .checkpoint(&group(), &stream(), 0, &owner_a(), lease_a.generation, 100)
            .await;
        assert!(matches!(result, Err(Error::OwnershipLost(_))));
    }

    #[tokio::test]
    async fn checkpoint_is_monotonic() {
        let coord = MemoryPartitionCoordinator::new();
        let lease = coord
            .claim(&group(), &stream(), 0, &owner_a(), short_lease())
            .await
            .unwrap()
            .unwrap();
        coord
            .checkpoint(&group(), &stream(), 0, &owner_a(), lease.generation, 100)
            .await
            .unwrap();
        coord
            .checkpoint(&group(), &stream(), 0, &owner_a(), lease.generation, 50)
            .await
            .unwrap();
        sleep(Duration::from_millis(5)).await;
        let new_lease = coord
            .claim(&group(), &stream(), 0, &owner_b(), long_lease())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(new_lease.checkpoint_sequence, 100);
    }
}
