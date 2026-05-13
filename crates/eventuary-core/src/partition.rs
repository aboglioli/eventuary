use std::num::{NonZeroU16, NonZeroU32};

use crate::error::{Error, Result};
use crate::event::Event;

const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

/// Logical partition assignment used by `PartitionedReader` and
/// `CheckpointReader` for in-process lane scheduling.
///
/// `id < count.get()` is enforced at construction.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct LogicalPartition {
    id: u16,
    count: NonZeroU16,
}

impl LogicalPartition {
    pub fn new(id: u16, count: NonZeroU16) -> Result<Self> {
        if id >= count.get() {
            return Err(Error::Config(format!(
                "logical partition id {id} out of range for count {count}",
                count = count.get()
            )));
        }
        Ok(Self { id, count })
    }

    pub fn id(&self) -> u16 {
        self.id
    }

    pub fn count(&self) -> u16 {
        self.count.get()
    }

    pub fn count_nz(&self) -> NonZeroU16 {
        self.count
    }
}

/// Cursor extension: report which logical partition a cursor belongs to.
pub trait CursorPartition {
    fn partition(&self) -> Option<LogicalPartition>;
}

/// Cursor extension: map the delivery cursor to the value that should be
/// persisted by a `CheckpointStore`. For most source cursors this is the
/// identity; partition wrappers strip the partition envelope so the store
/// commits only the underlying source cursor.
pub trait CommitCursor: Clone + Send + Sync + 'static {
    type Commit: Clone + Ord + Send + Sync + 'static;
    fn commit_cursor(&self) -> Self::Commit;
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

/// FNV-1a 64-bit hash. Drives every partition decision so a single hash
/// function is shared between [`EventKey::partition`] and the keyless
/// fallback in [`partition_for`].
pub fn fnv1a_u64(bytes: &[u8]) -> u64 {
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
    fn logical_partition_rejects_id_out_of_range() {
        let count = NonZeroU16::new(4).unwrap();
        let err = LogicalPartition::new(4, count).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn logical_partition_accessors_reflect_construction() {
        let count = NonZeroU16::new(8).unwrap();
        let p = LogicalPartition::new(3, count).unwrap();
        assert_eq!(p.id(), 3);
        assert_eq!(p.count(), 8);
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
