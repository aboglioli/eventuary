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
//! Schema setup is component-owned. Each component exposes a `Component::schema_sql(config)`
//! method (`SqliteWriter::schema_sql`, `SqliteCheckpointStore::schema_sql`, etc.) and
//! `Component::prepare_schema(conn, config)` to apply it.
//! `SqliteDatabase::open(...)` only opens a connection and does not create Eventuary tables.
//!
//! Also ships sqlite-backed implementations of the IO store traits:
//! - [`multiplexer::SqliteMultiplexerStore`]
//! - [`buffer::SqliteBufferStore`]
//! - [`dedupe::SqliteDedupeStore`]
//! - [`checkpoint::SqliteCheckpointStore`]
//! - [`watermark::SqliteWatermarkStore`]

pub mod buffer;
pub mod checkpoint;
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
