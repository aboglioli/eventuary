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
//! Schema setup is component-owned. Use each component's `Component::schema_sql(config)`
//! method (`PgWriter::schema_sql`, `PgReader::schema_sql`, `PgCheckpointStore::schema_sql`, …)
//! to generate DDL, and the corresponding `Component::connect(pool, config).await` or
//! `Component::prepare_schema(pool, config).await` to apply it.
//!
//! `PgDatabase::connect(...)` only opens a pool and does not create Eventuary tables.

pub mod buffer;
pub mod checkpoint;
pub mod claim_buffer;
pub mod coordinator;
pub mod database;
pub mod dedupe;
mod event_log;
pub mod multiplexer;
pub mod partitioning;
pub mod reader;
pub mod relation;
mod schema;
pub mod watermark;
pub mod writer;
