//! PostgreSQL event log backend for [eventuary](https://crates.io/crates/eventuary).
//!
//! `BIGSERIAL` sequence on an append-only events table, `(organization, sequence)`,
//! `(organization, topic, sequence)` and `(organization, namespace, sequence)`
//! indexes. Consumer offsets keyed by `(consumer_group_id, stream_id,
//! partition, partition_count)`.
//!
//! Polling reader (no LISTEN/NOTIFY in this version). ack advances the
//! checkpoint with a backwards-move guard, nack leaves it unchanged.
//!
//! Also ships postgres-backed implementations of the IO store traits:
//! - [`PgMultiplexerStore`]
//! - [`PgBufferStore`]
//! - [`PgDedupeStore`]
//! - [`PgCheckpointStore`]

pub mod buffer_store;
pub mod checkpoint_store;
pub mod database;
pub mod dedupe_store;
pub mod multiplexer_store;
pub mod reader;
pub mod relation;
pub mod writer;

pub use buffer_store::{PgBufferStore, PgBufferStoreConfig, PgBufferStoreId};
pub use checkpoint_store::{PgCheckpointStore, PgCheckpointStoreConfig};
pub use dedupe_store::{PgDedupeStore, PgDedupeStoreConfig};
pub use multiplexer_store::{PgMultiplexerStore, PgMultiplexerStoreConfig};
