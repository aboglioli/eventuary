//! SQLite event log backend for [eventuary](https://crates.io/crates/eventuary).
//!
//! Append-only events table with auto-incrementing sequence. Reader streams
//! events in sequence order and supports consumer groups via a
//! `consumer_offsets` table scoped by `(consumer_group_id, checkpoint_name,
//! partition, partition_count)`.
//!
//! ack advances the checkpoint, nack leaves it unchanged. SQLite work runs in
//! `tokio::task::spawn_blocking` to avoid blocking the async runtime.

mod checkpoint_store;
mod database;
mod reader;
mod relation;
mod writer;

pub use checkpoint_store::{SqliteCheckpointStore, SqliteCheckpointStoreConfig};
pub use database::{
    Migration, SqliteConn, SqliteDatabase, SqliteDatabaseConfig, migrations, render_migration_sql,
    render_schema_sql, schema_sql,
};
pub use reader::{
    SqliteCursor, SqliteCursorAcker, SqliteReader, SqliteReaderConfig, SqliteStream,
    SqliteSubscription,
};
pub use relation::SqliteRelationName;
pub use writer::{SqliteEventWriter, SqliteWriterConfig};
