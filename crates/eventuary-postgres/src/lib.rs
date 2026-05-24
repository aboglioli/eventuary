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
//! - [`PgWatermarkStore`]

pub mod buffer_store;
pub mod checkpoint_store;
pub mod claim_buffer_store;
pub mod coordinated_acker;
pub mod coordinated_reader;
pub mod database;
pub mod dedupe_store;
pub mod multiplexer_store;
pub mod partition_backfill;
pub mod partition_coordinator;
pub mod partition_cursor;
pub mod reader;
pub mod relation;
pub mod watermark_store;
pub mod writer;

pub use buffer_store::{PgBufferStore, PgBufferStoreConfig, PgBufferStoreId};
pub use checkpoint_store::{PgCheckpointStore, PgCheckpointStoreConfig};
pub use claim_buffer_store::{PgClaimedBufferStore, PgClaimedBufferStoreConfig};
pub use coordinated_acker::PgCoordinatedAcker;
pub use coordinated_reader::{
    PgCoordinatedReader, PgCoordinatedReaderConfig, PgCoordinatedStream, PgCoordinatedSubscription,
};
pub use dedupe_store::{PgDedupeStore, PgDedupeStoreConfig};
pub use multiplexer_store::{PgMultiplexerStore, PgMultiplexerStoreConfig};
pub use partition_backfill::{BackfillReport, PgPartitionBackfill, PgPartitionBackfillConfig};
pub use partition_coordinator::{PgPartitionCoordinator, PgPartitionCoordinatorConfig};
pub use partition_cursor::PgPartitionCursor;
pub use watermark_store::{PgWatermarkStore, PgWatermarkStoreConfig};
pub use writer::{PgPartitioningConfig, PgWriter, PgWriterConfig};
