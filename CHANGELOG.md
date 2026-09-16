# Changelog

All notable changes to this project are documented in this file. The format is
loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Breaking changes

- `eventuary-sqs` is renamed to `eventuary-aws` and now hosts every AWS
  backend. The umbrella feature `sqs` is renamed to `aws`, and the umbrella
  module moves from `eventuary::sqs` to `eventuary::aws`.
- Because the crate covers more than one AWS service, its modules nest by
  service first and role second. Update imports:
  - `eventuary::sqs::reader::{SqsReader, SqsSubscription, SqsReaderConfig}` →
    `eventuary::aws::sqs::reader::{...}`
  - `eventuary::sqs::writer::SqsWriter` → `eventuary::aws::sqs::writer::SqsWriter`
  - `eventuary::sqs::flusher::SqsFlusher` → `eventuary::aws::sqs::flusher::SqsFlusher`
- The `eventuary-sqs` crate is retired. Existing published versions keep
  working and are not yanked, but no new versions will be published; depend on
  `eventuary-aws` instead.

### Added

- `eventuary::aws::sns::writer::SnsWriter`: a `Writer` that publishes events to
  an SNS topic via `Publish` / `PublishBatch`, using the same `SerializedEvent`
  JSON wire format as every other durable backend. Batches are chunked to the
  10-entry / 256 KB `PublishBatch` limits.
- `SqsWriterConfig { queue_type: SqsQueueType }` and `SqsReaderConfig.queue_type`
  let the SQS writer and reader target standard or FIFO queues. `SqsWriter`
  previously set no `MessageGroupId`, so it could not write to a FIFO queue at
  all. FIFO reads attach a `ReceiveRequestAttemptId` so a retried receive
  returns the same messages instead of stalling the message group.
- `SnsWriterConfig { topic_type: SnsTopicType }` selects the topic the writer
  addresses. `Standard` (the default) sets no FIFO attributes; `Fifo` maps
  `MessageGroupId` from `Event::key()` and `MessageDeduplicationId` from
  `Event::id()`; `FifoContentBasedDeduplication` omits the deduplication id for
  topics that derive it themselves.
- Floci integration coverage for the SNS writer (single publish, batch
  chunking, multi-queue fanout, FIFO, oversized-payload rejection, end-to-end
  SNS → SQS delivery through `SqsReader`) and for previously untested SQS
  paths (`SqsFlusher` ack/nack, poison-record skipping, batch writes).

### Notes

- SNS is publish-only and ships no reader: SNS has no receive API. Consume
  published events by subscribing SQS queues to the topic and reading each with
  `SqsReader`.
- Queues subscribed to an SNS topic **must** have `RawMessageDelivery` enabled.
  Without it SNS wraps the body in a notification envelope that `SqsReader`
  cannot decode as a `SerializedEvent`, so every event is treated as a poison
  record and silently ack-skipped.

## [0.2.0] - 2026-05-29

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

### Fixed

- Raw `PgReader` and `SqliteReader` with `PartitionSelection::All` now decode
  rows whose `partition_id` / `partition_count` columns are `NULL` to a
  synthetic cursor partition `(id = 0, count = 1)` instead of erroring with
  `event has NULL partition columns`. This lets default-writer rows be read
  back without enabling inline partitioning.
- Partition-filtered reads (`PartitionSelection::One` / `Many`),
  `PartitionedReader::source_from_cursor`, and `CoordinatedReader` still
  require real partition columns: use inline writer partitioning or run
  `PgPartitionBackfill` / `SqlitePartitionBackfill` before relying on those
  flows. Mixed `NULL` / real partition rows on the same log split
  `CheckpointReader` state between the synthetic and real cursors; always
  backfill before enabling inline partitioning on a non-empty log.

### Documentation

- README and AGENTS updated to describe the current `PartitionedReaderConfig<P>`
  resolver/hasher pipeline and the SQL null-partition semantics.

## [0.1.0] - 2026-02-12

Initial public release.
