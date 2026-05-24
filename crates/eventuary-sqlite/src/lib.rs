//! SQLite event log backend for [eventuary](https://crates.io/crates/eventuary).
//!
//! Append-only events table with auto-incrementing sequence. Reader streams
//! events in sequence order and supports consumer groups via a
//! `consumer_offsets` table scoped by `(consumer_group_id, stream_id,
//! partition, partition_count)`.
//!
//! ack advances the checkpoint, nack leaves it unchanged. SQLite work runs in
//! `tokio::task::spawn_blocking` to avoid blocking the async runtime.
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
pub mod multiplexer_store;
pub mod partition_coordinator;
pub mod reader;
pub mod relation;
pub mod watermark_store;
pub mod writer;

pub use buffer_store::{SqliteBufferStore, SqliteBufferStoreConfig, SqliteBufferStoreId};
pub use checkpoint_store::{SqliteCheckpointStore, SqliteCheckpointStoreConfig};
pub use coordinated_reader::{
    SqliteCoordinatedAcker, SqliteCoordinatedCursor, SqliteCoordinatedReader,
    SqliteCoordinatedReaderConfig, SqliteCoordinatedStream, SqliteCoordinatedSubscription,
};
pub use dedupe_store::{SqliteDedupeStore, SqliteDedupeStoreConfig};
pub use multiplexer_store::{SqliteMultiplexerStore, SqliteMultiplexerStoreConfig};
pub use partition_coordinator::{SqlitePartitionCoordinator, SqlitePartitionCoordinatorConfig};
pub use watermark_store::{SqliteWatermarkStore, SqliteWatermarkStoreConfig};
