//! SQLite event log backend for [eventuary](https://crates.io/crates/eventuary).
//!
//! Provides an append-only events table with auto-incrementing sequence cursors
//! (`SqliteCursor`). `SqliteReader` is a source reader: its acker advances only
//! the active in-memory stream cursor, and nack leaves the row eligible for
//! redelivery by that stream. SQLite work runs in `tokio::task::spawn_blocking`
//! to avoid blocking the async runtime.
//!
//! Durable consumer progress is owned by `SqliteCheckpointStore` and composed
//! with `eventuary_core::io::reader::CheckpointReader`. Checkpoints are keyed by
//! `(consumer_group_id, stream_id, cursor_id)` and store the full cursor as JSON.
//!
//! Also ships sqlite-backed implementations of the IO store traits:
//! - [`SqliteMultiplexerStore`]
//! - [`SqliteBufferStore`]
//! - [`SqliteDedupeStore`]
//! - [`SqliteCheckpointStore`]
//! - [`SqliteWatermarkStore`]

pub mod buffer_store;
pub mod checkpoint_store;
pub mod coordinated_reader;
pub mod database;
pub mod dedupe_store;
pub mod event_log;
pub mod multiplexer_store;
pub mod partition_coordinator;
pub mod reader;
pub mod relation;
pub mod schema;
pub mod watermark_store;
pub mod writer;

pub use buffer_store::{SqliteBufferStore, SqliteBufferStoreConfig, SqliteBufferStoreId};
pub use checkpoint_store::{SqliteCheckpointStore, SqliteCheckpointStoreConfig};
pub use coordinated_reader::{
    SqliteCoordinatedAcker, SqliteCoordinatedCursor, SqliteCoordinatedReader,
    SqliteCoordinatedReaderConfig, SqliteCoordinatedStream, SqliteCoordinatedSubscription,
};
pub use dedupe_store::{SqliteDedupeStore, SqliteDedupeStoreConfig};
pub use event_log::{SqliteEventLogSchema, SqliteEventLogSchemaConfig};
pub use multiplexer_store::{SqliteMultiplexerStore, SqliteMultiplexerStoreConfig};
pub use partition_coordinator::{SqlitePartitionCoordinator, SqlitePartitionCoordinatorConfig};
pub use watermark_store::{SqliteWatermarkStore, SqliteWatermarkStoreConfig};
