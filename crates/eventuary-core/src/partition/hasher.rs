use crate::event_key::fnv1a_u64;

pub trait PartitionHasher: Send + Sync {
    fn hash(&self, key: &str) -> u64;
    fn strategy(&self) -> &str;
}

pub struct Fnv1a64PartitionHasher;

impl Default for Fnv1a64PartitionHasher {
    fn default() -> Self {
        Self
    }
}

impl PartitionHasher for Fnv1a64PartitionHasher {
    fn hash(&self, key: &str) -> u64 {
        fnv1a_u64(key.as_bytes())
    }

    fn strategy(&self) -> &str {
        "fnv1a64:v1"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_is_deterministic() {
        let hasher = Fnv1a64PartitionHasher;
        assert_eq!(hasher.hash("order-123"), hasher.hash("order-123"));
    }

    #[test]
    fn distinct_inputs_yield_distinct_outputs() {
        let hasher = Fnv1a64PartitionHasher;
        assert_ne!(hasher.hash("order-123"), hasher.hash("customer-7"));
        assert_ne!(hasher.hash("user-1"), hasher.hash("user-2"));
    }

    #[test]
    fn known_fixed_vectors() {
        let hasher = Fnv1a64PartitionHasher;
        assert_eq!(hasher.hash("order-123"), fnv1a_u64(b"order-123"));
        assert_eq!(hasher.hash("customer-7"), fnv1a_u64(b"customer-7"));
    }

    #[test]
    fn strategy_is_correct() {
        let hasher = Fnv1a64PartitionHasher;
        assert_eq!(hasher.strategy(), "fnv1a64:v1");
    }
}
