use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::Mutex;

use eventuary_core::io::reader::{
    CheckpointScope, Generation, PartitionCoordinator, PartitionLease,
};
use eventuary_core::io::{Cursor, OwnerId};
use eventuary_core::{Error, Partition, Result};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct ConsumerKey {
    scope: CheckpointScope,
    owner_id: OwnerId,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct PartitionStateKey {
    scope: CheckpointScope,
    partition_id: u16,
}

#[derive(Debug, Clone)]
struct PartitionState<C> {
    owner_id: Option<OwnerId>,
    lease_until: Option<DateTime<Utc>>,
    generation: Generation,
    checkpoint_cursor: Option<C>,
    partition_count: u16,
}

fn partition_count_mismatch_error(
    scope: &CheckpointScope,
    partition_id: u16,
    stored: u16,
    requested: u16,
) -> Error {
    Error::Config(format!(
        "partition count mismatch for scope {} stream {} partition {}: stored {}, requested {}",
        scope.consumer_group_id.as_str(),
        scope.stream_id.as_str(),
        partition_id,
        stored,
        requested,
    ))
}

#[derive(Debug)]
struct State<C> {
    consumers: HashMap<ConsumerKey, DateTime<Utc>>,
    partitions: HashMap<PartitionStateKey, PartitionState<C>>,
}

impl<C> Default for State<C> {
    fn default() -> Self {
        Self {
            consumers: HashMap::new(),
            partitions: HashMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct MemoryPartitionCoordinator<C> {
    state: Arc<Mutex<State<C>>>,
}

impl<C> Clone for MemoryPartitionCoordinator<C> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl<C> Default for MemoryPartitionCoordinator<C> {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
        }
    }
}

impl<C> MemoryPartitionCoordinator<C> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<C> PartitionCoordinator<C> for MemoryPartitionCoordinator<C>
where
    C: Cursor + Clone + Send + Sync + 'static,
{
    async fn heartbeat<'a>(
        &'a self,
        scope: &'a CheckpointScope,
        owner_id: &'a OwnerId,
        lease_duration: Duration,
    ) -> Result<()> {
        let key = ConsumerKey {
            scope: scope.clone(),
            owner_id: owner_id.clone(),
        };
        let lease_until = Utc::now() + lease_duration;
        self.state.lock().await.consumers.insert(key, lease_until);
        Ok(())
    }

    async fn live_consumers<'a>(&'a self, scope: &'a CheckpointScope) -> Result<usize> {
        let now = Utc::now();
        let state = self.state.lock().await;
        let count = state
            .consumers
            .iter()
            .filter(|(k, lease_until)| &k.scope == scope && **lease_until > now)
            .count();
        Ok(count)
    }

    async fn release_consumer<'a>(
        &'a self,
        scope: &'a CheckpointScope,
        owner_id: &'a OwnerId,
    ) -> Result<()> {
        let key = ConsumerKey {
            scope: scope.clone(),
            owner_id: owner_id.clone(),
        };
        self.state.lock().await.consumers.remove(&key);
        Ok(())
    }

    async fn claim<'a>(
        &'a self,
        scope: &'a CheckpointScope,
        owner_id: &'a OwnerId,
        partition: Partition,
        lease_duration: Duration,
    ) -> Result<Option<PartitionLease<C>>> {
        let key = PartitionStateKey {
            scope: scope.clone(),
            partition_id: partition.id(),
        };
        let now = Utc::now();
        let lease_until = now + lease_duration;
        let mut state = self.state.lock().await;

        match state.partitions.get_mut(&key) {
            None => {
                let generation = Generation::initial().next();
                let entry = PartitionState {
                    owner_id: Some(owner_id.clone()),
                    lease_until: Some(lease_until),
                    generation,
                    checkpoint_cursor: None,
                    partition_count: partition.count(),
                };
                let lease = PartitionLease {
                    scope: scope.clone(),
                    owner_id: owner_id.clone(),
                    partition,
                    generation,
                    checkpoint_cursor: None,
                    lease_until,
                };
                state.partitions.insert(key, entry);
                Ok(Some(lease))
            }
            Some(entry) => {
                if entry.partition_count != partition.count() {
                    return Err(partition_count_mismatch_error(
                        scope,
                        partition.id(),
                        entry.partition_count,
                        partition.count(),
                    ));
                }
                let is_expired = entry.lease_until.map(|t| t <= now).unwrap_or(true);
                let is_unowned = entry.owner_id.is_none();
                let is_self = entry.owner_id.as_ref() == Some(owner_id);

                if is_unowned || is_expired || is_self {
                    entry.generation = entry.generation.next();
                    entry.owner_id = Some(owner_id.clone());
                    entry.lease_until = Some(lease_until);
                    let lease = PartitionLease {
                        scope: scope.clone(),
                        owner_id: owner_id.clone(),
                        partition,
                        generation: entry.generation,
                        checkpoint_cursor: entry.checkpoint_cursor.clone(),
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
        lease: &'a PartitionLease<C>,
        lease_duration: Duration,
    ) -> Result<()> {
        let key = PartitionStateKey {
            scope: lease.scope.clone(),
            partition_id: lease.partition.id(),
        };
        let mut state = self.state.lock().await;
        if let Some(entry) = state.partitions.get(&key)
            && entry.partition_count != lease.partition.count()
        {
            return Err(partition_count_mismatch_error(
                &lease.scope,
                lease.partition.id(),
                entry.partition_count,
                lease.partition.count(),
            ));
        }
        match state.partitions.get_mut(&key) {
            Some(entry)
                if entry.owner_id.as_ref() == Some(&lease.owner_id)
                    && entry.generation == lease.generation =>
            {
                entry.lease_until = Some(Utc::now() + lease_duration);
                Ok(())
            }
            _ => Err(Error::OwnershipLost(format!(
                "partition {}: ownership lost or generation mismatch for owner {} gen {}",
                lease.partition.id(),
                lease.owner_id,
                lease.generation,
            ))),
        }
    }

    async fn release<'a>(&'a self, lease: &'a PartitionLease<C>) -> Result<()> {
        let key = PartitionStateKey {
            scope: lease.scope.clone(),
            partition_id: lease.partition.id(),
        };
        let mut state = self.state.lock().await;
        if let Some(entry) = state.partitions.get(&key)
            && entry.partition_count != lease.partition.count()
        {
            return Err(partition_count_mismatch_error(
                &lease.scope,
                lease.partition.id(),
                entry.partition_count,
                lease.partition.count(),
            ));
        }
        match state.partitions.get_mut(&key) {
            Some(entry)
                if entry.owner_id.as_ref() == Some(&lease.owner_id)
                    && entry.generation == lease.generation =>
            {
                entry.owner_id = None;
                entry.lease_until = None;
                entry.generation = entry.generation.next();
                Ok(())
            }
            _ => Err(Error::OwnershipLost(format!(
                "partition {}: ownership lost or generation mismatch for owner {} gen {}",
                lease.partition.id(),
                lease.owner_id,
                lease.generation,
            ))),
        }
    }

    /// Advance `checkpoint_cursor` to `cursor` only if `(owner_id, generation)` matches.
    ///
    /// The update is monotonic: if `cursor.order_key()` is less than or equal to
    /// the stored cursor's `order_key()`, the call is a silent no-op.
    async fn checkpoint<'a>(&'a self, lease: &'a PartitionLease<C>, cursor: C) -> Result<()> {
        let key = PartitionStateKey {
            scope: lease.scope.clone(),
            partition_id: lease.partition.id(),
        };
        let mut state = self.state.lock().await;
        if let Some(entry) = state.partitions.get(&key)
            && entry.partition_count != lease.partition.count()
        {
            return Err(partition_count_mismatch_error(
                &lease.scope,
                lease.partition.id(),
                entry.partition_count,
                lease.partition.count(),
            ));
        }
        match state.partitions.get_mut(&key) {
            Some(entry)
                if entry.owner_id.as_ref() == Some(&lease.owner_id)
                    && entry.generation == lease.generation =>
            {
                let should_advance = match &entry.checkpoint_cursor {
                    None => true,
                    Some(existing) => cursor.order_key() > existing.order_key(),
                };
                if should_advance {
                    entry.checkpoint_cursor = Some(cursor);
                }
                Ok(())
            }
            _ => Err(Error::OwnershipLost(format!(
                "partition {}: ownership lost or generation mismatch for owner {} gen {}",
                lease.partition.id(),
                lease.owner_id,
                lease.generation,
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::num::NonZeroU16;

    use serde::{Deserialize, Serialize};
    use tokio::time::sleep;

    use eventuary_core::io::cursor::CursorOrder;
    use eventuary_core::io::{ConsumerGroupId, StreamId};

    #[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
    struct TestCursor(i64);

    impl Cursor for TestCursor {
        fn order_key(&self) -> CursorOrder {
            CursorOrder::from_i64(self.0)
        }
    }

    fn scope() -> CheckpointScope {
        CheckpointScope::new(
            ConsumerGroupId::new("orders-projection-v1").unwrap(),
            StreamId::new("orders-events").unwrap(),
        )
    }

    fn partition(id: u16) -> Partition {
        Partition::new(id, NonZeroU16::new(8).unwrap()).unwrap()
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
        let coord = MemoryPartitionCoordinator::<TestCursor>::new();
        coord
            .heartbeat(&scope(), &owner_a(), long_lease())
            .await
            .unwrap();
        assert_eq!(coord.live_consumers(&scope()).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn live_consumers_excludes_expired() {
        let coord = MemoryPartitionCoordinator::<TestCursor>::new();
        coord
            .heartbeat(&scope(), &owner_a(), short_lease())
            .await
            .unwrap();
        sleep(Duration::from_millis(5)).await;
        assert_eq!(coord.live_consumers(&scope()).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn claim_free_partition_succeeds() {
        let coord = MemoryPartitionCoordinator::<TestCursor>::new();
        let lease = coord
            .claim(&scope(), &owner_a(), partition(0), long_lease())
            .await
            .unwrap();
        let lease = lease.expect("expected Some(lease)");
        assert_eq!(lease.generation, Generation::from_i64(1));
        assert_eq!(lease.checkpoint_cursor, None);
        assert_eq!(lease.owner_id, owner_a());
    }

    #[tokio::test]
    async fn claim_contested_returns_none() {
        let coord = MemoryPartitionCoordinator::<TestCursor>::new();
        coord
            .claim(&scope(), &owner_a(), partition(7), long_lease())
            .await
            .unwrap();
        let result = coord
            .claim(&scope(), &owner_b(), partition(7), long_lease())
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn claim_after_expiry_succeeds() {
        let coord = MemoryPartitionCoordinator::<TestCursor>::new();
        coord
            .claim(&scope(), &owner_a(), partition(0), short_lease())
            .await
            .unwrap();
        sleep(Duration::from_millis(5)).await;
        let lease = coord
            .claim(&scope(), &owner_b(), partition(0), long_lease())
            .await
            .unwrap();
        let lease = lease.expect("expected Some(lease) after expiry");
        assert_eq!(lease.generation, Generation::from_i64(2));
        assert_eq!(lease.owner_id, owner_b());
    }

    #[tokio::test]
    async fn renew_with_matching_generation() {
        let coord = MemoryPartitionCoordinator::<TestCursor>::new();
        let lease = coord
            .claim(&scope(), &owner_a(), partition(0), long_lease())
            .await
            .unwrap()
            .unwrap();
        coord.renew(&lease, long_lease()).await.unwrap();
    }

    #[tokio::test]
    async fn renew_with_stale_generation_returns_ownership_lost() {
        let coord = MemoryPartitionCoordinator::<TestCursor>::new();
        let lease_a = coord
            .claim(&scope(), &owner_a(), partition(0), short_lease())
            .await
            .unwrap()
            .unwrap();
        sleep(Duration::from_millis(5)).await;
        coord
            .claim(&scope(), &owner_b(), partition(0), long_lease())
            .await
            .unwrap();
        let result = coord.renew(&lease_a, long_lease()).await;
        assert!(matches!(result, Err(Error::OwnershipLost(_))));
    }

    #[tokio::test]
    async fn release_with_matching_generation() {
        let coord = MemoryPartitionCoordinator::<TestCursor>::new();
        let lease = coord
            .claim(&scope(), &owner_a(), partition(0), long_lease())
            .await
            .unwrap()
            .unwrap();
        coord.release(&lease).await.unwrap();
        let new_lease = coord
            .claim(&scope(), &owner_b(), partition(0), long_lease())
            .await
            .unwrap();
        let new_lease = new_lease.expect("expected claim to succeed after release");
        assert_eq!(new_lease.generation.get(), lease.generation.get() + 2);
    }

    #[tokio::test]
    async fn release_with_stale_generation_returns_ownership_lost() {
        let coord = MemoryPartitionCoordinator::<TestCursor>::new();
        let lease_a = coord
            .claim(&scope(), &owner_a(), partition(0), short_lease())
            .await
            .unwrap()
            .unwrap();
        sleep(Duration::from_millis(5)).await;
        coord
            .claim(&scope(), &owner_b(), partition(0), long_lease())
            .await
            .unwrap();
        let result = coord.release(&lease_a).await;
        assert!(matches!(result, Err(Error::OwnershipLost(_))));
    }

    #[tokio::test]
    async fn checkpoint_with_matching_generation_advances() {
        let coord = MemoryPartitionCoordinator::<TestCursor>::new();
        let lease = coord
            .claim(&scope(), &owner_a(), partition(0), short_lease())
            .await
            .unwrap()
            .unwrap();
        coord.checkpoint(&lease, TestCursor(100)).await.unwrap();
        sleep(Duration::from_millis(5)).await;
        let new_lease = coord
            .claim(&scope(), &owner_b(), partition(0), long_lease())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(new_lease.checkpoint_cursor, Some(TestCursor(100)));
    }

    #[tokio::test]
    async fn checkpoint_with_stale_generation_returns_ownership_lost() {
        let coord = MemoryPartitionCoordinator::<TestCursor>::new();
        let lease_a = coord
            .claim(&scope(), &owner_a(), partition(0), short_lease())
            .await
            .unwrap()
            .unwrap();
        sleep(Duration::from_millis(5)).await;
        coord
            .claim(&scope(), &owner_b(), partition(0), long_lease())
            .await
            .unwrap();
        let result = coord.checkpoint(&lease_a, TestCursor(100)).await;
        assert!(matches!(result, Err(Error::OwnershipLost(_))));
    }

    #[tokio::test]
    async fn claim_rejects_partition_count_mismatch() {
        let coord: MemoryPartitionCoordinator<TestCursor> = MemoryPartitionCoordinator::new();
        let s = scope();
        let owner_a = OwnerId::new("worker-a").unwrap();
        let owner_b = OwnerId::new("worker-b").unwrap();

        let count_four = NonZeroU16::new(4).unwrap();
        let count_eight = NonZeroU16::new(8).unwrap();
        let p_four = Partition::new(0, count_four).unwrap();
        let p_eight = Partition::new(0, count_eight).unwrap();

        coord
            .claim(&s, &owner_a, p_four, Duration::from_millis(1))
            .await
            .unwrap()
            .unwrap();
        sleep(Duration::from_millis(5)).await;

        let err = coord
            .claim(&s, &owner_b, p_eight, Duration::from_secs(1))
            .await
            .unwrap_err();

        assert!(
            matches!(err, Error::Config(message) if message.contains("partition count mismatch"))
        );
    }

    #[tokio::test]
    async fn checkpoint_is_monotonic() {
        let coord = MemoryPartitionCoordinator::<TestCursor>::new();
        let lease = coord
            .claim(&scope(), &owner_a(), partition(0), short_lease())
            .await
            .unwrap()
            .unwrap();
        coord.checkpoint(&lease, TestCursor(100)).await.unwrap();
        coord.checkpoint(&lease, TestCursor(50)).await.unwrap();
        sleep(Duration::from_millis(5)).await;
        let new_lease = coord
            .claim(&scope(), &owner_b(), partition(0), long_lease())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(new_lease.checkpoint_cursor, Some(TestCursor(100)));
    }
}
