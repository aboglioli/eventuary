# eventuary-sqlite

SQLite event backend for [eventuary](https://crates.io/crates/eventuary). Provides a durable single-node event log with consumer-group checkpointing, suitable for embedded deployments and local development.

Schema setup is component-owned. `SqliteWriter::connect(conn, config)` prepares only the event-log table, while `SqliteDedupeStore::connect(conn, config)` prepares only the dedupe table. `SqliteDatabase::open(...)` only opens a connection and does not create Eventuary tables.
