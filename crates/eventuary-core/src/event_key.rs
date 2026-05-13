use std::fmt;
use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::partition::fnv1a_u64;

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

    /// Stable partition id in `0..total_partitions` derived from the key bytes.
    pub fn partition(&self, total_partitions: NonZeroU32) -> u32 {
        (fnv1a_u64(self.0.as_bytes()) % u64::from(total_partitions.get())) as u32
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
        let partitions = NonZeroU32::new(16).unwrap();
        let first = key.partition(partitions);
        let second = key.partition(partitions);
        assert_eq!(first, second);
    }

    #[test]
    fn partition_stays_in_range() {
        let partitions = NonZeroU32::new(8).unwrap();
        for i in 0..1024 {
            let key = EventKey::new(format!("key-{i}")).unwrap();
            assert!(key.partition(partitions) < 8);
        }
    }

    #[test]
    fn same_key_same_partition() {
        let partitions = NonZeroU32::new(32).unwrap();
        let a = EventKey::new("order-7").unwrap();
        let b = EventKey::new("order-7").unwrap();
        assert_eq!(a.partition(partitions), b.partition(partitions));
    }

    #[test]
    fn fnv_known_vector_remains_stable() {
        let key = EventKey::new("user-42").unwrap();
        let partitions = NonZeroU32::new(16).unwrap();
        assert_eq!(key.partition(partitions), 11);
    }
}
