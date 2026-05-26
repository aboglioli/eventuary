use std::fmt;
use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::event::Event;
use crate::payload::Payload;

const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

pub(crate) fn fnv1a_u64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(FNV_OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    })
}

#[derive(
    Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Partition {
    id: u32,
    count: NonZeroU32,
}

impl Partition {
    pub fn new(id: u32, count: NonZeroU32) -> Result<Self> {
        if id >= count.get() {
            return Err(Error::Config(format!(
                "partition id {id} out of range for count {count}",
                count = count.get()
            )));
        }
        Ok(Self { id, count })
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn count(&self) -> u32 {
        self.count.get()
    }

    pub fn count_nz(&self) -> NonZeroU32 {
        self.count
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct PartitionKey(String);

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

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PartitionHash(u64);

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

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct PartitionStrategy(String);

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

/// Validated set of partitions sharing the same `partition_count`.
///
/// Constructed via `PartitionGroup::new` so the invariants hold:
/// - non-empty
/// - all partitions share the same `count`
///
/// Used by `PartitionSelection::Many` so a single reader read can fetch
/// across several owned lanes in one query (`partition_id IN (...)` /
/// `partition_id = ANY(...)`) instead of one query per lane.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PartitionGroup {
    partitions: Vec<Partition>,
}

impl PartitionGroup {
    pub fn new(partitions: Vec<Partition>) -> Result<Self> {
        if partitions.is_empty() {
            return Err(Error::Config(
                "partition group must not be empty".to_owned(),
            ));
        }
        let count = partitions[0].count_nz();
        if partitions.iter().any(|p| p.count_nz() != count) {
            return Err(Error::Config(
                "all partitions in a group must share the same count".to_owned(),
            ));
        }
        Ok(Self { partitions })
    }

    /// Build a singleton group containing exactly one partition. Infallible:
    /// a single-element group trivially satisfies the non-empty and uniform
    /// `partition_count` invariants.
    pub fn singleton(partition: Partition) -> Self {
        Self {
            partitions: vec![partition],
        }
    }

    /// Build a `PartitionGroup` from any iterator of `Partition`s, validating
    /// that all share the same `partition_count`. Returns an error if the
    /// iterator is empty or `partition_count` values mismatch.
    pub fn new_from_iter(partitions: impl IntoIterator<Item = Partition>) -> Result<Self> {
        Self::new(partitions.into_iter().collect())
    }

    pub fn partitions(&self) -> &[Partition] {
        &self.partitions
    }

    pub fn count_nz(&self) -> NonZeroU32 {
        self.partitions[0].count_nz()
    }

    pub fn count(&self) -> u32 {
        self.count_nz().get()
    }

    pub fn len(&self) -> usize {
        self.partitions.len()
    }

    pub fn is_empty(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub enum PartitionSelection {
    #[default]
    All,
    One(Partition),
    Many(PartitionGroup),
}

pub trait PartitionKeyResolver<P = Payload>: Send + Sync + 'static {
    fn partition_key(&self, event: &Event<P>) -> Result<PartitionKey>;
}

pub trait PartitionHasher: Send + Sync + 'static {
    fn hash(&self, key: &PartitionKey) -> PartitionHash;
    fn strategy(&self) -> &str;

    fn partition_for(&self, key: &PartitionKey, count: NonZeroU32) -> Partition {
        let hash = self.hash(key);
        Partition::new((hash.get() % count.get() as u64) as u32, count)
            .expect("hash modulo count is always in range")
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Fnv1a64PartitionHasher;

impl PartitionHasher for Fnv1a64PartitionHasher {
    fn hash(&self, key: &PartitionKey) -> PartitionHash {
        PartitionHash::new(fnv1a_u64(key.as_str().as_bytes()))
    }

    fn strategy(&self) -> &str {
        "fnv1a64:v1"
    }
}

mod composite;
mod event_key;
mod metadata;
mod namespace;
mod organization;
mod topic;

pub use composite::CompositePartitionKeyResolver;
pub use event_key::EventKeyPartitionKeyResolver;
pub use metadata::MetadataPartitionKeyResolver;
pub use namespace::NamespacePartitionKeyResolver;
pub use organization::OrganizationPartitionKeyResolver;
pub use topic::TopicPartitionKeyResolver;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_rejects_id_out_of_range() {
        let count = NonZeroU32::new(4).unwrap();
        let err = Partition::new(4, count).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn partition_accessors_reflect_construction() {
        let count = NonZeroU32::new(8).unwrap();
        let p = Partition::new(3, count).unwrap();
        assert_eq!(p.id(), 3);
        assert_eq!(p.count(), 8);
    }

    #[test]
    fn partition_serializes_as_id_and_count() {
        let p = Partition::new(3, NonZeroU32::new(8).unwrap()).unwrap();
        let value = serde_json::to_value(p).unwrap();
        assert_eq!(value["id"], 3);
        assert_eq!(value["count"], 8);
        let decoded: Partition = serde_json::from_value(value).unwrap();
        assert_eq!(decoded, p);
    }

    #[test]
    fn partition_supports_ids_beyond_u16_range() {
        let id = u16::MAX as u32 + 1;
        let count = NonZeroU32::new(u32::MAX).unwrap();
        let p = Partition::new(id, count).unwrap();
        assert_eq!(p.id(), id);
        assert_eq!(p.count(), u32::MAX);
        let value = serde_json::to_value(p).unwrap();
        let decoded: Partition = serde_json::from_value(value).unwrap();
        assert_eq!(decoded, p);
    }

    #[test]
    fn fnv1a_known_vector_remains_stable() {
        // Anchors the stable hash used by Fnv1a64PartitionHasher across versions.
        let count = NonZeroU32::new(16).unwrap();
        let id = (fnv1a_u64("user-42".as_bytes()) % count.get() as u64) as u32;
        assert_eq!(id, 11);
    }

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
    fn partition_strategy_display_matches_as_str() {
        let strategy = PartitionStrategy::new("event_key_or_id:fnv1a64:v1").unwrap();
        assert_eq!(strategy.to_string(), strategy.as_str());
    }

    #[test]
    fn partition_selection_default_is_all() {
        assert_eq!(PartitionSelection::default(), PartitionSelection::All);
    }

    #[test]
    fn partition_selection_one_constructs_correctly() {
        let count = NonZeroU32::new(4).unwrap();
        let p = Partition::new(2, count).unwrap();
        let sel = PartitionSelection::One(p);
        assert_eq!(sel, PartitionSelection::One(p));
    }

    #[test]
    fn partition_group_rejects_empty() {
        let err = PartitionGroup::new(Vec::new()).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn partition_group_rejects_mismatched_counts() {
        let p1 = Partition::new(0, NonZeroU32::new(4).unwrap()).unwrap();
        let p2 = Partition::new(0, NonZeroU32::new(8).unwrap()).unwrap();
        let err = PartitionGroup::new(vec![p1, p2]).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn partition_group_accepts_uniform_counts() {
        let count = NonZeroU32::new(8).unwrap();
        let group = PartitionGroup::new(vec![
            Partition::new(0, count).unwrap(),
            Partition::new(3, count).unwrap(),
            Partition::new(7, count).unwrap(),
        ])
        .unwrap();
        assert_eq!(group.len(), 3);
        assert_eq!(group.count(), 8);
        assert_eq!(group.partitions().len(), 3);
    }

    #[test]
    fn partition_group_singleton_holds_single_partition() {
        let count = NonZeroU32::new(4).unwrap();
        let partition = Partition::new(2, count).unwrap();
        let group = PartitionGroup::singleton(partition);
        assert_eq!(group.len(), 1);
        assert_eq!(group.partitions(), &[partition]);
        assert_eq!(group.count_nz(), count);
    }

    #[test]
    fn partition_group_new_from_iter_accepts_uniform_counts() {
        let count = NonZeroU32::new(8).unwrap();
        let group = PartitionGroup::new_from_iter([
            Partition::new(0, count).unwrap(),
            Partition::new(5, count).unwrap(),
        ])
        .unwrap();
        assert_eq!(group.len(), 2);
        assert_eq!(group.count(), 8);
    }

    #[test]
    fn partition_group_new_from_iter_rejects_empty() {
        let err = PartitionGroup::new_from_iter(std::iter::empty()).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn partition_group_new_from_iter_rejects_mismatched_counts() {
        let p1 = Partition::new(0, NonZeroU32::new(4).unwrap()).unwrap();
        let p2 = Partition::new(0, NonZeroU32::new(8).unwrap()).unwrap();
        let err = PartitionGroup::new_from_iter([p1, p2]).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn partition_selection_many_constructs_from_group() {
        let count = NonZeroU32::new(4).unwrap();
        let group = PartitionGroup::new(vec![
            Partition::new(0, count).unwrap(),
            Partition::new(1, count).unwrap(),
        ])
        .unwrap();
        let sel = PartitionSelection::Many(group.clone());
        assert_eq!(sel, PartitionSelection::Many(group));
    }

    #[test]
    fn hasher_hash_is_deterministic() {
        let hasher = Fnv1a64PartitionHasher;
        let key = PartitionKey::new("order-123").unwrap();
        assert_eq!(hasher.hash(&key), hasher.hash(&key));
    }

    #[test]
    fn hasher_distinct_inputs_yield_distinct_outputs() {
        let hasher = Fnv1a64PartitionHasher;
        let k1 = PartitionKey::new("order-123").unwrap();
        let k2 = PartitionKey::new("customer-7").unwrap();
        let k3 = PartitionKey::new("user-1").unwrap();
        let k4 = PartitionKey::new("user-2").unwrap();
        assert_ne!(hasher.hash(&k1), hasher.hash(&k2));
        assert_ne!(hasher.hash(&k3), hasher.hash(&k4));
    }

    #[test]
    fn hasher_known_fixed_vectors() {
        let hasher = Fnv1a64PartitionHasher;
        let k1 = PartitionKey::new("order-123").unwrap();
        let k2 = PartitionKey::new("customer-7").unwrap();
        assert_eq!(hasher.hash(&k1).get(), 0x1b96f9c28b5d5aba);
        assert_eq!(hasher.hash(&k2).get(), 0x660308ab83c6fdf1);
    }

    #[test]
    fn hasher_strategy_is_correct() {
        let hasher = Fnv1a64PartitionHasher;
        assert_eq!(hasher.strategy(), "fnv1a64:v1");
    }

    #[test]
    fn hasher_partition_for_returns_modulo_partition() {
        let hasher = Fnv1a64PartitionHasher;
        let key = PartitionKey::new("order-123").unwrap();
        let partition = hasher.partition_for(&key, NonZeroU32::new(64).unwrap());
        let expected_id = (0x1b96f9c28b5d5aba_u64 % 64) as u32;
        assert_eq!(partition.id(), expected_id);
        assert_eq!(partition.count(), 64);
    }
}
