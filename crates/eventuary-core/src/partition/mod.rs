mod composite;
mod event_key;
mod hasher;
mod metadata;
mod namespace;
mod organization;
mod topic;
mod types;

use crate::error::Result;
use crate::event::Event;

pub trait PartitionKeyResolver: Send + Sync + 'static {
    fn partition_key(&self, event: &Event) -> Result<types::PartitionKey>;
}

// FixedPartition variant is intentionally deferred: it requires bypassing the hasher,
// which lives outside this trait. Reintroduce when writer-side partitioning gains a
// dedicated path for explicit partition ids.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum UnkeyedPartitionMode {
    Error,
    EventId,
}

pub use self::hasher::{Fnv1a64PartitionHasher, PartitionHasher};
pub use self::types::{PartitionHash, PartitionKey, PartitionSelection, PartitionStrategy};
pub use crate::event_key::Partition;
pub use composite::CompositePartitionKeyResolver;
pub use event_key::EventKeyPartitionKeyResolver;
pub use metadata::MetadataPartitionKeyResolver;
pub use namespace::NamespacePartitionKeyResolver;
pub use organization::OrganizationPartitionKeyResolver;
pub use topic::TopicPartitionKeyResolver;
