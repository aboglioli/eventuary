//! SQLite event log backend for [eventuary](https://crates.io/crates/eventuary).
//!
//! Append-only events table with auto-incrementing sequence. Reader streams
//! events in sequence order and supports consumer groups via a
//! `consumer_offsets` table scoped by `(consumer_group_id, checkpoint_name)`.
//!
//! ack advances the checkpoint, nack leaves it unchanged. SQLite work runs in
//! `tokio::task::spawn_blocking` to avoid blocking the async runtime.

mod database;
mod reader;
mod writer;

pub use database::{Migration, SqliteConn, SqliteDatabase, migrations, schema_sql};
pub use reader::{SqliteAcker, SqliteAckerVariant, SqliteReader, SqliteReaderConfig, SqliteStream};
pub use writer::SqliteEventWriter;
