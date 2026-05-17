use std::fmt;
use std::num::NonZeroU16;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

pub(crate) fn fnv1a_u64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(FNV_OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    })
}

/// A logical partition assignment: an `id` within a `count`.
/// `id < count.get()` is enforced at construction.
#[derive(
    Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Partition {
    id: u16,
    count: NonZeroU16,
}

impl Partition {
    pub fn new(id: u16, count: NonZeroU16) -> Result<Self> {
        if id >= count.get() {
            return Err(Error::Config(format!(
                "partition id {id} out of range for count {count}",
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct EventKey(String);

impl EventKey {
    pub fn new(s: impl Into<String>) -> Result<Self> {
        let s = s.into();
        if s.is_empty() {
            return Err(Error::InvalidEventKey("must not be empty".into()));
        }
        if s.len() > 1024 {
            return Err(Error::InvalidEventKey(
                "event key must not exceed 1024 characters".into(),
            ));
        }
        Ok(Self(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Determine the partition for this key within a given count.
    pub fn partition_for(&self, count: NonZeroU16) -> Partition {
        let id = (fnv1a_u64(self.0.as_bytes()) % count.get() as u64) as u16;
        Partition::new(id, count).expect("id < count by modulo")
    }
}

impl fmt::Display for EventKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<String> for EventKey {
    type Error = Error;
    fn try_from(s: String) -> Result<Self> {
        Self::new(s)
    }
}

impl From<EventKey> for String {
    fn from(k: EventKey) -> Self {
        k.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_key() {
        assert!(EventKey::new("task-123").is_ok());
        assert!(EventKey::new("agent-abc").is_ok());
        assert!(EventKey::new("org/project/name").is_ok());
    }

    #[test]
    fn empty_key_fails() {
        assert!(EventKey::new("").is_err());
    }

    #[test]
    fn too_long_key_fails() {
        let s = "a".repeat(1025);
        assert!(EventKey::new(s).is_err());
    }

    #[test]
    fn partition_is_deterministic() {
        let key = EventKey::new("user-42").unwrap();
        let partitions = NonZeroU16::new(16).unwrap();
        let first = key.partition_for(partitions);
        let second = key.partition_for(partitions);
        assert_eq!(first, second);
    }

    #[test]
    fn partition_stays_in_range() {
        let partitions = NonZeroU16::new(8).unwrap();
        for i in 0..1024 {
            let key = EventKey::new(format!("key-{i}")).unwrap();
            assert!(key.partition_for(partitions).id() < 8);
        }
    }

    #[test]
    fn same_key_same_partition() {
        let partitions = NonZeroU16::new(32).unwrap();
        let a = EventKey::new("order-7").unwrap();
        let b = EventKey::new("order-7").unwrap();
        assert_eq!(a.partition_for(partitions), b.partition_for(partitions));
    }

    #[test]
    fn fnv_known_vector_remains_stable() {
        let key = EventKey::new("user-42").unwrap();
        let partitions = NonZeroU16::new(16).unwrap();
        assert_eq!(key.partition_for(partitions).id(), 11);
    }

    #[test]
    fn partition_rejects_id_out_of_range() {
        let count = NonZeroU16::new(4).unwrap();
        let err = Partition::new(4, count).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn partition_accessors_reflect_construction() {
        let count = NonZeroU16::new(8).unwrap();
        let p = Partition::new(3, count).unwrap();
        assert_eq!(p.id(), 3);
        assert_eq!(p.count(), 8);
    }

    #[test]
    fn partition_serializes_as_id_and_count() {
        let p = Partition::new(3, NonZeroU16::new(8).unwrap()).unwrap();
        let value = serde_json::to_value(p).unwrap();
        assert_eq!(value["id"], 3);
        assert_eq!(value["count"], 8);
        let decoded: Partition = serde_json::from_value(value).unwrap();
        assert_eq!(decoded, p);
    }
}
