//! SQLite event log backend for [eventuary](https://crates.io/crates/eventuary).
//!
//! Append-only events table with auto-incrementing sequence. Reader streams
//! events in sequence order and supports consumer groups via a
//! `consumer_offsets` table scoped by `(consumer_group_id, stream_id,
//! partition, partition_count)`.
//!
//! ack advances the checkpoint, nack leaves it unchanged. SQLite work runs in
//! `tokio::task::spawn_blocking` to avoid blocking the async runtime.

pub mod checkpoint_store;
pub mod database;
pub mod reader;
pub mod relation;
pub mod writer;

pub use checkpoint_store::SqliteCheckpointStore;
pub use reader::SqliteReader;
pub use writer::SqliteEventWriter;
