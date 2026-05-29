# Changelog

All notable changes to this project are documented in this file. The format is
loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Breaking changes

- Backend crates now expose concrete implementations through role modules only.
  Root convenience reexports such as `eventuary::postgres::PgWriter` were
  removed. Use module paths such as `eventuary::postgres::writer::PgWriter`,
  `eventuary::postgres::reader::PgReader`,
  `eventuary::postgres::checkpoint::PgCheckpointStore`, and
  `eventuary::postgres::coordinator::PgPartitionCoordinator`.
- Backend store modules were renamed to role modules without the `_store`
  suffix: `checkpoint_store` → `checkpoint`, `partition_coordinator` →
  `coordinator`, `buffer_store` → `buffer`, `claim_buffer_store` →
  `claim_buffer`, `dedupe_store` → `dedupe`, `multiplexer_store` →
  `multiplexer`, `watermark_store` → `watermark`, `subscriber_work_store` →
  `subscriber_work`, and `partition_backfill` → `partitioning`.
- SQL coordinated-reader aliases moved from `backend::coordinated_reader::*`
  into `backend::reader::*` (`PgCoordinatedReader`, `PgCoordinatedSubscription`,
  `PgCoordinatedReaderConfig`, `PgCoordinatedAcker`, `PgCoordinatedStreamAcker`,
  `PgCoordinatedCursor`, `PgCoordinatedStream`, `PgPartitionedCursor`, and the
  matching `Sqlite*` aliases).
- SQS and Kafka reader configs moved from `backend::reader_config::*` into
  `backend::reader::*`: `eventuary::sqs::reader::SqsReaderConfig` and
  `eventuary::kafka::reader::KafkaReaderConfig`.
- `PgEventLogSchema`, `PgEventLogSchemaConfig`, `SqliteEventLogSchema`, and
  `SqliteEventLogSchemaConfig` are now private (`pub(crate)`). Render the
  event-log DDL through the component that owns it:
  `PgWriter::schema_sql(&PgWriterConfig)`,
  `PgReader::schema_sql(&PgReaderConfig)`,
  `SqliteWriter::schema_sql(&SqliteWriterConfig)`,
  `SqliteReader::schema_sql(&SqliteReaderConfig)`, etc.
- Low-level migration helpers (`Migration`, `RelationReplacement`,
  `render_migration_sql`, `render_schema_sql`, `apply_schema`) are no longer
  part of the public API. Each backend component owns its own schema lifecycle.

## [0.1.0] - 2026-02-12

Initial public release.
