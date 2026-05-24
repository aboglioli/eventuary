use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::event_key::Partition;

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct PartitionKey(String);

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PartitionHash(u64);

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct PartitionStrategy(String);

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub enum PartitionSelection {
    #[default]
    All,
    One(Partition),
}

impl PartitionKey {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(Error::Config("partition key must not be empty".to_owned()));
        }
        if value.len() > 1024 {
            return Err(Error::Config(
                "partition key must not exceed 1024 characters".to_owned(),
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PartitionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<String> for PartitionKey {
    type Error = Error;
    fn try_from(s: String) -> Result<Self> {
        Self::new(s)
    }
}

impl From<PartitionKey> for String {
    fn from(k: PartitionKey) -> Self {
        k.0
    }
}

impl PartitionHash {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn get(self) -> u64 {
        self.0
    }

    pub fn to_sql_i64(self) -> i64 {
        self.0 as i64
    }

    pub fn from_sql_i64(value: i64) -> Self {
        Self(value as u64)
    }
}

impl fmt::Display for PartitionHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl PartitionStrategy {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(Error::Config(
                "partition strategy must not be empty".to_owned(),
            ));
        }
        if value.len() > 128 {
            return Err(Error::Config(
                "partition strategy must not exceed 128 characters".to_owned(),
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PartitionStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<String> for PartitionStrategy {
    type Error = Error;
    fn try_from(s: String) -> Result<Self> {
        Self::new(s)
    }
}

impl From<PartitionStrategy> for String {
    fn from(s: PartitionStrategy) -> Self {
        s.0
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU16;

    use super::*;

    #[test]
    fn partition_key_accepts_valid() {
        assert!(PartitionKey::new("tenant-abc/stream").is_ok());
    }

    #[test]
    fn partition_key_rejects_empty() {
        assert!(matches!(PartitionKey::new(""), Err(Error::Config(_))));
    }

    #[test]
    fn partition_key_rejects_over_limit() {
        let s = "a".repeat(1025);
        assert!(matches!(PartitionKey::new(s), Err(Error::Config(_))));
    }

    #[test]
    fn partition_key_accepts_exactly_1024() {
        let s = "a".repeat(1024);
        assert!(PartitionKey::new(s).is_ok());
    }

    #[test]
    fn partition_key_json_round_trip() {
        let key = PartitionKey::new("my-key").unwrap();
        let json = serde_json::to_string(&key).unwrap();
        let decoded: PartitionKey = serde_json::from_str(&json).unwrap();
        assert_eq!(key, decoded);
    }

    #[test]
    fn partition_key_display_matches_as_str() {
        let key = PartitionKey::new("some-key").unwrap();
        assert_eq!(key.to_string(), key.as_str());
    }

    #[test]
    fn partition_hash_sql_round_trip_zero() {
        let h = PartitionHash::new(0u64);
        assert_eq!(PartitionHash::from_sql_i64(h.to_sql_i64()), h);
    }

    #[test]
    fn partition_hash_sql_round_trip_max() {
        let h = PartitionHash::new(u64::MAX);
        assert_eq!(PartitionHash::from_sql_i64(h.to_sql_i64()), h);
    }

    #[test]
    fn partition_hash_sql_round_trip_sign_bit() {
        let h = PartitionHash::new(0x_8000_0000_0000_0000_u64);
        assert_eq!(h.to_sql_i64(), i64::MIN);
        assert_eq!(PartitionHash::from_sql_i64(h.to_sql_i64()), h);
    }

    #[test]
    fn partition_hash_serde_transparent() {
        let h = PartitionHash::new(42);
        let value = serde_json::to_value(h).unwrap();
        assert_eq!(value, serde_json::json!(42));
        let decoded: PartitionHash = serde_json::from_value(value).unwrap();
        assert_eq!(decoded, h);
    }

    #[test]
    fn partition_strategy_accepts_valid() {
        assert!(PartitionStrategy::new("fnv1a-mod").is_ok());
    }

    #[test]
    fn partition_strategy_rejects_empty() {
        assert!(matches!(PartitionStrategy::new(""), Err(Error::Config(_))));
    }

    #[test]
    fn partition_strategy_rejects_over_limit() {
        let s = "b".repeat(129);
        assert!(matches!(PartitionStrategy::new(s), Err(Error::Config(_))));
    }

    #[test]
    fn partition_strategy_accepts_exactly_128() {
        let s = "b".repeat(128);
        assert!(PartitionStrategy::new(s).is_ok());
    }

    #[test]
    fn partition_strategy_json_round_trip() {
        let strategy = PartitionStrategy::new("round-robin").unwrap();
        let json = serde_json::to_string(&strategy).unwrap();
        let decoded: PartitionStrategy = serde_json::from_str(&json).unwrap();
        assert_eq!(strategy, decoded);
    }

    #[test]
    fn partition_selection_default_is_all() {
        assert_eq!(PartitionSelection::default(), PartitionSelection::All);
    }

    #[test]
    fn partition_selection_one_constructs_correctly() {
        let count = NonZeroU16::new(4).unwrap();
        let p = Partition::new(2, count).unwrap();
        let sel = PartitionSelection::One(p);
        assert_eq!(sel, PartitionSelection::One(p));
    }
}
