//! PostgreSQL event log backend for [eventuary](https://crates.io/crates/eventuary).
//!
//! `BIGSERIAL` sequence on an append-only events table, `(organization, sequence)`,
//! `(organization, topic, sequence)` and `(organization, namespace, sequence)`
//! indexes. Consumer offsets keyed by `(consumer_group_id, checkpoint_name,
//! partition, partition_count)`.
//!
//! Polling reader (no LISTEN/NOTIFY in this version). ack advances the
//! checkpoint with a backwards-move guard, nack leaves it unchanged.

mod database;
mod reader;
mod writer;

pub use database::{Migration, PgConnectOptions, PgDatabase, migrations, schema_sql};
pub use reader::{PgAcker, PgAckerVariant, PgReader, PgReaderConfig, PgStream};
pub use writer::PgEventWriter;
