//! PostgreSQL event log backend for [eventuary](https://crates.io/crates/eventuary).
//!
//! Provides an append-only events table backed by `sqlx::PgPool` with source
//! cursors (`PgCursor`) ordered by `BIGSERIAL` sequence. `PgReader` is a source
//! reader: its acker advances only the active in-memory stream cursor, and nack
//! leaves the row eligible for redelivery by that stream.
//!
//! Durable consumer progress is owned by `PgCheckpointStore` and composed with
//! `eventuary_core::io::reader::CheckpointReader`. Checkpoints are keyed by
//! `(consumer_group_id, stream_id, cursor_id)` and store the full cursor as JSON.
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
pub mod coordinated_reader;
pub mod database;
pub mod dedupe_store;
pub mod multiplexer_store;
pub mod partition_backfill;
pub mod partition_coordinator;
pub mod reader;
pub mod relation;
pub mod watermark_store;
pub mod writer;

pub use buffer_store::{PgBufferStore, PgBufferStoreConfig, PgBufferStoreId};
pub use checkpoint_store::{PgCheckpointStore, PgCheckpointStoreConfig};
pub use claim_buffer_store::{PgClaimedBufferStore, PgClaimedBufferStoreConfig};
pub use coordinated_reader::{
    PgCoordinatedAcker, PgCoordinatedCursor, PgCoordinatedReader, PgCoordinatedReaderConfig,
    PgCoordinatedStream, PgCoordinatedSubscription,
};
pub use dedupe_store::{PgDedupeStore, PgDedupeStoreConfig};
pub use multiplexer_store::{PgMultiplexerStore, PgMultiplexerStoreConfig};
pub use partition_backfill::{BackfillReport, PgPartitionBackfill, PgPartitionBackfillConfig};
pub use partition_coordinator::{PgPartitionCoordinator, PgPartitionCoordinatorConfig};
pub use watermark_store::{PgWatermarkStore, PgWatermarkStoreConfig};
pub use writer::{PgPartitioningConfig, PgWriter, PgWriterConfig};
