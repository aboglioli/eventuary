use std::fmt;
use std::future::Future;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::Partition;
use crate::Result;
use crate::io::reader::checkpoint::CheckpointScope;
use crate::io::{Cursor, OwnerId};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Generation(i64);

impl Generation {
    pub fn initial() -> Self {
        Self(0)
    }

    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }

    pub fn get(self) -> i64 {
        self.0
    }

    pub fn from_i64(value: i64) -> Self {
        Self(value)
    }
}

impl fmt::Display for Generation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A lease on a single partition granted by a [`PartitionCoordinator`].
///
/// `checkpoint_cursor` encodes the last durable progress position for this
/// partition. Backends vary in how they represent "no prior progress":
/// - Memory: `None` for a freshly-claimed partition, `Some(C)` after the
///   first `checkpoint` call.
/// - Postgres and SQLite: always `Some(C)` — the cursor is constructed from
///   the stored `checkpoint_sequence` column (which defaults to `0` on
///   insert). Higher layers such as `CoordinatedReader` interpret a
///   sequence of `0` as "start from the subscription's initial position".
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PartitionLease<C> {
    pub scope: CheckpointScope,
    pub owner_id: OwnerId,
    pub partition: Partition,
    pub generation: Generation,
    pub checkpoint_cursor: Option<C>,
    pub lease_until: DateTime<Utc>,
}

pub trait PartitionCoordinator<C>: Clone + Send + Sync + 'static
where
    C: Cursor + Clone + Send + Sync + 'static,
{
    fn heartbeat<'a>(
        &'a self,
        scope: &'a CheckpointScope,
        owner_id: &'a OwnerId,
        lease_duration: Duration,
    ) -> impl Future<Output = Result<()>> + Send + 'a;

    fn live_consumers<'a>(
        &'a self,
        scope: &'a CheckpointScope,
    ) -> impl Future<Output = Result<usize>> + Send + 'a;

    /// Attempt to take a partition. Returns `Ok(Some(lease))` on success or
    /// `Ok(None)` if another live owner holds it. Increments `generation` on
    /// every successful claim.
    fn claim<'a>(
        &'a self,
        scope: &'a CheckpointScope,
        owner_id: &'a OwnerId,
        partition: Partition,
        lease_duration: Duration,
    ) -> impl Future<Output = Result<Option<PartitionLease<C>>>> + Send + 'a;

    /// Extend `lease_until` only if `(owner_id, generation)` still matches.
    /// Returns `Err(Error::OwnershipLost(_))` on mismatch.
    fn renew<'a>(
        &'a self,
        lease: &'a PartitionLease<C>,
        lease_duration: Duration,
    ) -> impl Future<Output = Result<()>> + Send + 'a;

    /// Clear `owner_id` and `lease_until` only if `(owner_id, generation)`
    /// matches. Increments `generation`. Returns `Err(Error::OwnershipLost(_))`
    /// on mismatch.
    fn release<'a>(
        &'a self,
        lease: &'a PartitionLease<C>,
    ) -> impl Future<Output = Result<()>> + Send + 'a;

    /// Write `cursor` only if `(owner_id, generation)` matches. The update
    /// is monotonic via `Cursor::order_key()`. Returns
    /// `Err(Error::OwnershipLost(_))` on mismatch.
    fn checkpoint<'a>(
        &'a self,
        lease: &'a PartitionLease<C>,
        cursor: C,
    ) -> impl Future<Output = Result<()>> + Send + 'a;
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU16;

    use super::*;
    use crate::io::{ConsumerGroupId, StreamId};

    #[test]
    fn generation_initial_is_zero() {
        assert_eq!(Generation::initial().get(), 0);
    }

    #[test]
    fn generation_next_increments() {
        let g = Generation::initial();
        assert_eq!(g.next().get(), 1);
        assert_eq!(g.next().next().get(), 2);
    }

    #[test]
    fn generation_ordering() {
        assert!(Generation::initial() < Generation::initial().next());
        assert!(Generation::from_i64(5) > Generation::from_i64(3));
    }

    #[test]
    fn generation_serde_transparent() {
        let g = Generation::from_i64(7);
        let v = serde_json::to_value(g).unwrap();
        assert_eq!(v, serde_json::json!(7));
        let back: Generation = serde_json::from_value(v).unwrap();
        assert_eq!(back, g);
    }

    #[test]
    fn error_ownership_lost_variant_exists() {
        let err = crate::Error::OwnershipLost("test".to_owned());
        assert!(matches!(err, crate::Error::OwnershipLost(_)));
    }

    #[test]
    fn partition_lease_constructs_and_equals() {
        let scope = CheckpointScope::new(
            ConsumerGroupId::new("my-group").unwrap(),
            StreamId::new("orders").unwrap(),
        );
        let partition = Partition::new(3, NonZeroU16::new(8).unwrap()).unwrap();
        let lease: PartitionLease<i64> = PartitionLease {
            scope,
            owner_id: OwnerId::new("worker-01").unwrap(),
            partition,
            generation: Generation::from_i64(1),
            checkpoint_cursor: Some(42),
            lease_until: DateTime::from_timestamp(0, 0).unwrap(),
        };
        let cloned = lease.clone();
        assert_eq!(lease, cloned);
    }
}
