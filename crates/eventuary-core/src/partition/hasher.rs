use std::num::NonZeroU16;

use crate::event_key::{Partition, fnv1a_u64};
use crate::partition::types::{PartitionHash, PartitionKey};

pub trait PartitionHasher: Send + Sync + 'static {
    fn hash(&self, key: &PartitionKey) -> PartitionHash;
    fn strategy(&self) -> &str;

    fn partition_for(&self, key: &PartitionKey, count: NonZeroU16) -> Partition {
        let hash = self.hash(key);
        Partition::new((hash.get() % count.get() as u64) as u16, count)
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

#[cfg(test)]
mod tests {
    use std::num::NonZeroU16;

    use super::*;

    #[test]
    fn hash_is_deterministic() {
        let hasher = Fnv1a64PartitionHasher;
        let key = PartitionKey::new("order-123").unwrap();
        assert_eq!(hasher.hash(&key), hasher.hash(&key));
    }

    #[test]
    fn distinct_inputs_yield_distinct_outputs() {
        let hasher = Fnv1a64PartitionHasher;
        let k1 = PartitionKey::new("order-123").unwrap();
        let k2 = PartitionKey::new("customer-7").unwrap();
        let k3 = PartitionKey::new("user-1").unwrap();
        let k4 = PartitionKey::new("user-2").unwrap();
        assert_ne!(hasher.hash(&k1), hasher.hash(&k2));
        assert_ne!(hasher.hash(&k3), hasher.hash(&k4));
    }

    #[test]
    fn known_fixed_vectors() {
        let hasher = Fnv1a64PartitionHasher;
        let k1 = PartitionKey::new("order-123").unwrap();
        let k2 = PartitionKey::new("customer-7").unwrap();
        assert_eq!(hasher.hash(&k1).get(), 0x1b96f9c28b5d5aba);
        assert_eq!(hasher.hash(&k2).get(), 0x660308ab83c6fdf1);
    }

    #[test]
    fn strategy_is_correct() {
        let hasher = Fnv1a64PartitionHasher;
        assert_eq!(hasher.strategy(), "fnv1a64:v1");
    }

    #[test]
    fn partition_for_returns_modulo_partition() {
        let hasher = Fnv1a64PartitionHasher;
        let key = PartitionKey::new("order-123").unwrap();
        let partition = hasher.partition_for(&key, NonZeroU16::new(64).unwrap());
        let expected_id = (0x1b96f9c28b5d5aba_u64 % 64) as u16;
        assert_eq!(partition.id(), expected_id);
        assert_eq!(partition.count(), 64);
    }
}
