//! PostgreSQL event log backend for [eventuary](https://crates.io/crates/eventuary).
//!
//! `BIGSERIAL` sequence on an append-only events table, `(organization, sequence)`,
//! `(organization, topic, sequence)` and `(organization, namespace, sequence)`
//! indexes. Consumer offsets keyed by `(consumer_group_id, checkpoint_name,
//! partition, partition_count)`.
//!
//! Polling reader (no LISTEN/NOTIFY in this version). ack advances the
//! checkpoint with a backwards-move guard, nack leaves it unchanged.

mod checkpoint_store;
mod database;
mod reader;
mod relation;
mod writer;

pub use checkpoint_store::{PgCheckpointStore, PgCheckpointStoreConfig};
pub use database::{
    Migration, PgConnectOptions, PgDatabase, PgDatabaseConfig, migrations, render_migration_sql,
    render_schema_sql, schema_sql,
};
pub use reader::{PgCursor, PgCursorAcker, PgReader, PgReaderConfig, PgStream, PgSubscription};
pub use relation::PgRelationName;
pub use writer::{PgEventWriter, PgWriterConfig};
