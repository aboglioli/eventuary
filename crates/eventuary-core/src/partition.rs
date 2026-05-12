use std::num::NonZeroU32;

use crate::error::{Error, Result};
use crate::event::Event;

const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

/// Hashable contributor for partition routing.
///
/// Implementations should hash a stable byte representation of the value
/// (e.g. an event key) into a partition id in `0..total_partitions`.
pub trait PartitionKey {
    fn partition(&self, total_partitions: NonZeroU32) -> u32;
}

/// Runtime partition assignment for a reader.
///
/// `count` is the total number of partitions in the assignment scheme;
/// `id` is the partition this assignment owns. The unpartitioned case is
/// represented by `Option::None` at the call site — not by `count == 1`.
///
/// Two workers using assignments with the same `count` and different `id`
/// receive disjoint slices of the event log when paired with
/// [`partition_for`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct PartitionAssignment {
    count: NonZeroU32,
    id: u32,
}

impl PartitionAssignment {
    /// Creates a new partition assignment.
    ///
    /// Rejects `count < 2` (use `Option::None` for the unpartitioned case)
    /// and `id >= count` (out of range).
    pub fn new(count: u32, id: u32) -> Result<Self> {
        if count < 2 {
            return Err(Error::Config(
                "partition_count must be > 1; use Option::None for unpartitioned".to_owned(),
            ));
        }
        let count_nz = NonZeroU32::new(count).expect("count > 1");
        if id >= count {
            return Err(Error::Config(format!(
                "partition id {id} out of range for count {count}"
            )));
        }
        Ok(Self {
            count: count_nz,
            id,
        })
    }

    pub fn count(&self) -> u32 {
        self.count.get()
    }

    pub fn count_nz(&self) -> NonZeroU32 {
        self.count
    }

    pub fn id(&self) -> u32 {
        self.id
    }
}

/// Canonical event → partition mapping.
///
/// Uses [`Event::key`] when present so events for the same logical entity
/// always route to the same partition. Falls back to the event id's bytes
/// when the event is keyless — the spread is even but not sticky to any
/// caller-visible identity.
///
/// Both paths share the same FNV-1a u64 hash via [`fnv1a_u64`], so the
/// modulo result is determined entirely by the input bytes and `count`.
pub fn partition_for(event: &Event, count: NonZeroU32) -> u32 {
    if let Some(key) = event.key() {
        return key.partition(count);
    }
    let id = event.id();
    let bytes = id.as_uuid().as_bytes();
    (fnv1a_u64(bytes) % u64::from(count.get())) as u32
}

/// FNV-1a 64-bit hash. Shared between [`PartitionKey`] implementors and
/// the keyless fallback in [`partition_for`] so a single hash function
/// drives every partition decision.
pub(crate) fn fnv1a_u64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(FNV_OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EventKey, Payload};

    fn ev_with_key(key: &str) -> Event {
        Event::builder("acme", "/x", "thing.happened", Payload::from_string("p"))
            .unwrap()
            .key(key)
            .unwrap()
            .build()
            .expect("valid event")
    }

    fn ev_keyless() -> Event {
        Event::builder("acme", "/x", "thing.happened", Payload::from_string("p"))
            .unwrap()
            .build()
            .expect("valid event")
    }

    #[test]
    fn assignment_rejects_count_below_two() {
        let err = PartitionAssignment::new(1, 0).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
        let err = PartitionAssignment::new(0, 0).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn assignment_rejects_id_out_of_range() {
        let err = PartitionAssignment::new(4, 4).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
        let err = PartitionAssignment::new(4, 99).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn assignment_accessors_reflect_construction() {
        let a = PartitionAssignment::new(8, 3).unwrap();
        assert_eq!(a.count(), 8);
        assert_eq!(a.id(), 3);
        assert_eq!(a.count_nz().get(), 8);
    }

    #[test]
    fn keyed_event_partition_matches_event_key_partition() {
        let event = ev_with_key("invoice-123");
        let count = NonZeroU32::new(8).unwrap();
        let key = EventKey::new("invoice-123").unwrap();
        assert_eq!(partition_for(&event, count), key.partition(count));
    }

    #[test]
    fn keyed_event_partition_stable_across_runs() {
        let event = ev_with_key("invoice-123");
        let count = NonZeroU32::new(8).unwrap();
        let first = partition_for(&event, count);
        let second = partition_for(&event, count);
        assert_eq!(first, second);
    }

    #[test]
    fn keyless_event_partition_in_range() {
        let count = NonZeroU32::new(8).unwrap();
        for _ in 0..256 {
            let event = ev_keyless();
            assert!(partition_for(&event, count) < 8);
        }
    }

    #[test]
    fn keyless_event_partition_spreads_evenly() {
        let count = NonZeroU32::new(4).unwrap();
        let mut buckets = [0usize; 4];
        for _ in 0..4096 {
            let event = ev_keyless();
            buckets[partition_for(&event, count) as usize] += 1;
        }
        let min = *buckets.iter().min().unwrap();
        let max = *buckets.iter().max().unwrap();
        assert!(min > 800, "bucket distribution too uneven: {buckets:?}");
        assert!(max < 1300, "bucket distribution too uneven: {buckets:?}");
    }

    #[test]
    fn every_event_lands_in_exactly_one_partition() {
        let event = ev_with_key("entity-xyz");
        for n in 2u32..=32 {
            let count = NonZeroU32::new(n).unwrap();
            let pid = partition_for(&event, count);
            assert!(pid < n);
            let mut hits = 0;
            for id in 0..n {
                if pid == id {
                    hits += 1;
                }
            }
            assert_eq!(hits, 1);
        }
    }
}
