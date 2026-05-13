//! Conformance test suite for eventuary backends.
//!
//! Being rewired as part of the cursor reader refactor: backend adapters
//! provide a per-backend subscription factory rather than a universal
//! `EventSubscription`. Capability flags remain available; the case suite
//! is being rebuilt to compose `PartitionedReader` and `CheckpointReader`
//! over each backend's source-cursor reader.

mod capabilities;

pub use capabilities::Capabilities;
