# Changelog

All notable changes to this project are documented in this file. The format is
loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Breaking changes

- `SqsReaderConfig` drops `start_from` and `consumer_group_id`. Neither could
  hold a value SQS could honour, so both existed only to be rejected by
  `validate` at runtime; removing them turns that into a compile error. SQS
  still has no replay cursor, and the queue URL remains the consumer identity.
- `SqsReaderConfig::limit` is now honoured instead of rejected. The reader
  already stopped the stream after `limit` deliveries when a `SqsSubscription`
  carried one; only the config validator disagreed.
- `AckBufferConfig` takes its values through `AckBufferConfig::new(max_pending,
  flush_interval)` instead of public fields, and `max_pending` is a
  `NonZeroUsize`. A zero `flush_interval` is now a supported setting meaning
  "run no timer", matching what `CheckpointFlushPolicy` already means by it:
  the buffer then flushes when `max_pending` tokens are held, and when it
  closes. `SqsReaderConfig` and `KafkaReaderConfig` no longer reject a zero
  `ack_buffer.max_pending`, because the type no longer permits one.

### Fixed

- A zero `AckBufferConfig::flush_interval` no longer kills acking. It reached
  `tokio::time::interval`, which panics on a zero period, and the panic stayed
  inside the spawned flusher task: the handle survived, acks kept queueing, and
  nothing ever flushed them. On SQS that meant `DeleteMessageBatch` never ran
  and every message redelivered forever; on Kafka, offsets never committed.

- `SqsReader` and `KafkaReader` log a message or record they cannot decode into
  an `Event`, at warn with its identity and the decode cause, before discarding
  it. Both previously discarded the error and wrote nothing, which made the
  documented `RawMessageDelivery` failure — a queue draining while the handler
  stays idle — impossible to diagnose from the outside.
- `SqsReader::read` validates the `SqsSubscription` it is given. Validation
  previously ran only in `SqsReader::new`, so a caller using the `Reader` trait
  with its own subscription got an opaque AWS error instead of `Error::Config`.

## [0.3.0-rc.1] - 2026-09-16

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

- `eventuary-fs`, a filesystem event log backend behind the umbrella `fs`
  feature (`eventuary::fs`). It stores a partitioned, segmented, append-only
  log on ordinary files with sparse offset and time indexes, and needs no
  server, driver, or C dependency. Records are JSON lines carrying a flat
  `offset` field, so a log stays readable with `cat`, `grep` and `jq`.
  - `fs::writer::FsWriter` and `fs::reader::FsReader` implement `Writer` and
    `Reader`. `FsSubscription` supports start and stop positions, filters,
    partition selection, and per-partition resume.
  - `FsCursor { partition, offset }` implements `Cursor` and `HasPartition`, so
    offsets compose with `PartitionedReader::source_from_cursor` and
    `CheckpointReader` the way the SQL cursors do.
  - `fs::checkpoint::FsCheckpointStore` and `fs::coordinator::FsPartitionCoordinator`
    give the backend durable consumer progress and multi-instance partition
    ownership under `(owner_id, generation)` fenced leases, with
    `fs::reader::FsCoordinatedReader` composing the two.
  - `fs::buffer`, `fs::dedupe`, `fs::multiplexer` and `fs::watermark` implement
    the remaining reader and handler store traits, and `fs::log::PartitionLog`
    exposes the storage engine directly.
  - `SyncPolicy` defaults to one fsync per megabyte appended. `RetentionPolicy`
    selects whole segments to drop by age or total size, reclaimed when the
    application calls `FsWriter::enforce_retention`.
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

- An `eventuary-fs` log has one producer process: `FsWriter::open` takes an
  exclusive advisory lock on every partition, which is what keeps offsets dense
  enough to use as cursors. Consumers are unrestricted and share a log through
  `FsPartitionCoordinator`. Because coordination uses advisory file locks, it is
  single-node; a shared network filesystem does not make it multi-host.
- A consumer whose `eventuary-fs` checkpoint falls behind the retained range
  fails with `Error::InvalidCursor` naming the partition and the number of
  events removed, rather than silently resuming at the new log start.
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
