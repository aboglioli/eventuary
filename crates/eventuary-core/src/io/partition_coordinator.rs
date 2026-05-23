use std::future::Future;
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::Result;
use crate::io::{ConsumerGroupId, OwnerId, StreamId};

pub trait PartitionCoordinator: Clone + Send + Sync + 'static {
    /// Upsert `(consumer_group_id, stream_id, owner_id)` with `lease_until = now + lease_duration`.
    fn heartbeat<'a>(
        &'a self,
        consumer_group_id: &'a ConsumerGroupId,
        stream_id: &'a StreamId,
        owner_id: &'a OwnerId,
        lease_duration: Duration,
    ) -> impl Future<Output = Result<()>> + Send + 'a;

    /// Count rows where `lease_until > now`.
    fn live_consumers<'a>(
        &'a self,
        consumer_group_id: &'a ConsumerGroupId,
        stream_id: &'a StreamId,
    ) -> impl Future<Output = Result<usize>> + Send + 'a;

    /// Attempt to take a partition. Returns `Ok(Some(lease))` on success (new generation, fresh
    /// lease, current checkpoint_sequence). Returns `Ok(None)` if another live owner holds it.
    /// Increments `generation` on every successful claim.
    fn claim<'a>(
        &'a self,
        consumer_group_id: &'a ConsumerGroupId,
        stream_id: &'a StreamId,
        partition_id: u16,
        owner_id: &'a OwnerId,
        lease_duration: Duration,
    ) -> impl Future<Output = Result<Option<PartitionLease>>> + Send + 'a;

    /// Extend `lease_until` only if `(owner_id, generation)` still matches. On generation
    /// mismatch (ownership lost), return `Err(Error::OwnershipLost(_))`.
    fn renew<'a>(
        &'a self,
        consumer_group_id: &'a ConsumerGroupId,
        stream_id: &'a StreamId,
        partition_id: u16,
        owner_id: &'a OwnerId,
        generation: i64,
        lease_duration: Duration,
    ) -> impl Future<Output = Result<()>> + Send + 'a;

    /// Clear `owner_id` and `lease_until` only if `(owner_id, generation)` matches. Increments
    /// `generation`. On mismatch, return `Err(Error::OwnershipLost(_))`.
    fn release<'a>(
        &'a self,
        consumer_group_id: &'a ConsumerGroupId,
        stream_id: &'a StreamId,
        partition_id: u16,
        owner_id: &'a OwnerId,
        generation: i64,
    ) -> impl Future<Output = Result<()>> + Send + 'a;

    /// Write `checkpoint_sequence` only if `(owner_id, generation)` matches. On mismatch, return
    /// `Err(Error::OwnershipLost(_))`.
    fn checkpoint<'a>(
        &'a self,
        consumer_group_id: &'a ConsumerGroupId,
        stream_id: &'a StreamId,
        partition_id: u16,
        owner_id: &'a OwnerId,
        generation: i64,
        sequence: i64,
    ) -> impl Future<Output = Result<()>> + Send + 'a;
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PartitionLease {
    pub consumer_group_id: ConsumerGroupId,
    pub stream_id: StreamId,
    pub partition_id: u16,
    pub owner_id: OwnerId,
    pub generation: i64,
    pub checkpoint_sequence: i64,
    pub lease_until: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::StreamId;

    #[test]
    fn partition_lease_constructs_and_equals() {
        let lease = PartitionLease {
            consumer_group_id: ConsumerGroupId::new("my-group").unwrap(),
            stream_id: StreamId::new("orders").unwrap(),
            partition_id: 3,
            owner_id: OwnerId::new("worker-01").unwrap(),
            generation: 1,
            checkpoint_sequence: 42,
            lease_until: DateTime::from_timestamp(0, 0).unwrap(),
        };
        let cloned = lease.clone();
        assert_eq!(lease, cloned);
    }

    #[test]
    fn error_ownership_lost_variant_exists() {
        let err = crate::Error::OwnershipLost("test".to_owned());
        assert!(matches!(err, crate::Error::OwnershipLost(_)));
    }
}
