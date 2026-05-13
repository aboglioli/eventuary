//! Backend conformance test suite for eventuary.
//!
//! Provides reusable test cases that every event backend should pass. Each
//! backend's integration test crate creates a [`Backend`] impl and calls
//! [`run_all`].

mod capabilities;
mod cases;
mod factory;

pub use capabilities::Capabilities;
pub use cases::{
    case_ack_advances_checkpoint, case_all_organizations_read,
    case_independent_checkpoints_within_group, case_independent_consumer_groups,
    case_nack_does_not_advance_checkpoint, case_namespace_prefix_filter, case_ordering_preserved,
    case_runtime_partition_checkpoint_independence, case_runtime_partition_isolates_workers,
    case_runtime_partition_per_key_stickiness, case_runtime_partition_unsupported_rejects,
    case_start_from_earliest, case_start_from_latest, case_start_from_timestamp, case_topic_filter,
    case_write_all_preserves_all_events, case_write_read_roundtrip, run_all,
};
pub use factory::{AckFn, AckFuture, Backend, ConsumerEvent, ReaderRequest};
