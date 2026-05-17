# Eventuary

Eventuary is a Rust event toolkit for logs, queues, streams, routing, replay,
checkpointing, and acknowledgements across multiple backends.

It provides a small typed event model, async IO traits, composable reader
wrappers, and optional backend implementations for in-memory channels, SQLite,
PostgreSQL, AWS SQS, and Apache Kafka. Everything intended for application use is
available through the `eventuary` umbrella crate, with backends enabled by Cargo
features.

> **Status:** Alpha (`0.1.0-alpha.0`). API may change before `0.1.0`.

Eventuary is a library you embed in your application. It is not a server, broker,
daemon, or transport runtime.

## Install

Most applications should depend on the umbrella crate and enable the backend
features they need. No backend is enabled by default.

```toml
[dependencies]
eventuary = { version = "0.1.0-alpha.0", features = ["postgres"] }
```

| Feature | Module | Backend crate |
|---------|--------|---------------|
| `memory` | `eventuary::memory` | [`eventuary-memory`](crates/eventuary-memory) |
| `sqlite` | `eventuary::sqlite` | [`eventuary-sqlite`](crates/eventuary-sqlite) |
| `postgres` | `eventuary::postgres` | [`eventuary-postgres`](crates/eventuary-postgres) |
| `sqs` | `eventuary::sqs` | [`eventuary-sqs`](crates/eventuary-sqs) |
| `kafka` | `eventuary::kafka` | [`eventuary-kafka`](crates/eventuary-kafka) |

The umbrella crate re-exports `eventuary-core` at its root:

```rust
use eventuary::{Event, Payload, Topic, Namespace, OrganizationId};
use eventuary::io::{Reader, Writer, Handler, Acker, Message};
```

Backend implementation types live under their owning backend module:

```rust
use eventuary::postgres::reader::{PgReader, PgSubscription, PgReaderConfig};
use eventuary::postgres::writer::{PgWriter, PgWriterConfig};
use eventuary::postgres::checkpoint_store::{PgCheckpointStore, PgCheckpointStoreConfig};
use eventuary::postgres::database::{PgDatabase, PgDatabaseConfig};
use eventuary::postgres::relation::PgRelationName;
```

### Direct sub-crate use

Advanced users can depend on sub-crates directly:

| Crate | When to depend directly |
|-------|-------------------------|
| [`eventuary-core`](crates/eventuary-core) | Building a custom backend or using only the event model and IO traits |
| [`eventuary-conformance`](crates/eventuary-conformance) | Backend-author dev-dependency for shared conformance scaffolding |
| [`eventuary-memory`](crates/eventuary-memory), etc. | Pinning or publishing against a backend crate independently |

Applications should normally use the umbrella crate.

## Workspace Layout

```text
crates/
├── eventuary/              # umbrella crate; re-exports core + feature-gated backends
├── eventuary-core/         # event model, serialization, IO traits, reader wrappers, consumer driver
├── eventuary-memory/       # tokio::mpsc backend for tests/dev/single-process use
├── eventuary-sqlite/       # rusqlite append-only event log + checkpoint store
├── eventuary-postgres/     # sqlx/Postgres append-only event log + checkpoint store
├── eventuary-sqs/          # AWS SQS writer/reader with batched delete acks
├── eventuary-kafka/        # rdkafka writer/reader with batched offset commits
└── eventuary-conformance/  # backend conformance support types
```

Layering rules:

- `eventuary-core` does not depend on any backend crate.
- Backend crates depend on `eventuary-core`, not on the umbrella crate.
- The umbrella crate contains no original implementation code; it only re-exports
  `eventuary-core` and optional backend crates.
- Backend `lib.rs` files expose modules only. Implementation types stay at
  paths such as `eventuary::sqlite::reader::SqliteReader`, not flat paths.

## Core Event Model

```rust
use eventuary::{Event, Payload};

let event = Event::builder(
    "acme",
    "/billing",
    "invoice.created",
    Payload::from_json(&serde_json::json!({ "amount": 100 }))?,
)?
.key("invoice-123")?
.correlation_id("billing-run-7")?
.build()?;
```

Important model types:

- `Event` — immutable event record with UUID v7 `id`, tenant organization,
  namespace, topic, payload, metadata, timestamp, version, optional key, and
  optional lineage fields.
- `Payload` — JSON, plain text, or binary content. Internally it uses
  `bytes::Bytes`, so cloning a payload is cheap and does not copy the byte
  buffer.
- `Topic` — dot-separated lowercase topic name with digits, `_`, and `-`.
- `Namespace` — slash-rooted hierarchy such as `/`, `/billing`, or
  `/billing/invoices`.
- `OrganizationId` — tenant scope; `_platform` is reserved for platform-wide
  events.
- `EventKey` — flexible non-empty key used for event keys, correlation IDs, and
  causation IDs. It provides deterministic FNV-1a partitioning.
- `Metadata` — validated string key/value metadata.
- `SerializedEvent` — stable wire format used by every backend.

## Async IO Model

Eventuary uses native async functions in traits. It does not use
`async-trait`.

```rust
use eventuary::{Event, Result};
use eventuary::io::Writer;

async fn emit<W: Writer>(writer: &W, event: &Event) -> Result<()> {
    writer.write(event).await
}
```

Main IO traits:

| Trait | Purpose |
|-------|---------|
| `Writer` | Append or send events to a backend |
| `Reader` | Open a subscription and return a stream of messages |
| `Acker` | Backend-specific `ack` / `nack` behavior |
| `Handler` | Process a borrowed `&Event` |
| `Filter` | Match or reject events |
| `Cursor` | Identify source progress for replay/checkpointing |

`Reader` returns `Message<A, C>`, where `A` is the backend acker and `C` is the
backend cursor:

```rust
use eventuary::io::{Acker, Cursor, Message};

async fn process<A, C>(message: Message<A, C>) -> eventuary::Result<()>
where
    A: Acker,
    C: Cursor,
{
    println!("topic = {}", message.event().topic());
    message.ack().await
}
```

The handler receives only `&Event`; ack/nack and cursor state remain in the
message envelope and are handled by the consumer driver or caller.

### Dynamic dispatch

Every IO trait has a dyn bridge and boxed/arc aliases:

| Static trait | Dyn trait | Box alias | Arc alias |
|--------------|-----------|-----------|-----------|
| `Writer` | `DynWriter` | `BoxWriter` | `ArcWriter` |
| `Reader` | `DynReader<S, C>` | `BoxReader<S, C>` | `ArcReader<S, C>` |
| `Handler` | `DynHandler` | `BoxHandler` | `ArcHandler` |
| `Acker` | `DynAcker` | `BoxAcker` | `ArcAcker` |
| `Filter` | — | `BoxFilter` | `ArcFilter` |

Use extension traits to erase concrete implementations:

```rust
use eventuary::io::{BoxWriter, WriterExt};
use eventuary::memory::writer::MemoryWriter;

let (tx, _rx) = tokio::sync::mpsc::channel(100);
let writer: BoxWriter = MemoryWriter::new(tx).into_boxed();
```

## In-Memory Backend Example

```toml
[dependencies]
eventuary = { version = "0.1.0-alpha.0", features = ["memory"] }
```

```rust
use eventuary::{Event, Payload, Result};
use eventuary::io::{Reader, Writer};
use eventuary::memory::reader::{MemoryReader, MemorySubscription};
use eventuary::memory::writer::MemoryWriter;
use futures::StreamExt;
use tokio::sync::mpsc;

async fn example() -> Result<()> {
    let (tx, rx) = mpsc::channel(100);
    let writer = MemoryWriter::new(tx);
    let reader = MemoryReader::new(rx);

    let event = Event::builder(
        "acme",
        "/orders",
        "order.placed",
        Payload::from_json(&serde_json::json!({ "total": 42 }))?,
    )?
    .key("order-1")?
    .build()?;

    writer.write(&event).await?;

    let mut stream = reader.read(MemorySubscription { limit: Some(1) }).await?;
    let message = stream.next().await.expect("one message")?;
    assert_eq!(message.event().topic().as_str(), "order.placed");
    message.ack().await?;

    Ok(())
}
```

The memory backend is intentionally simple: one `tokio::mpsc` channel, no
durable replay, and no-op ack/nack.

## Backend Overview

| Backend | Writer | Reader | Cursor | Durable checkpoint store | Typical use |
|---------|--------|--------|--------|---------------------------|-------------|
| memory | `memory::writer::MemoryWriter` | `memory::reader::MemoryReader` | `NoCursor` | no | tests, dev, single-process flows |
| SQLite | `sqlite::writer::SqliteWriter` | `sqlite::reader::SqliteReader` | `SqliteCursor` | yes | embedded durable event log |
| PostgreSQL | `postgres::writer::PgWriter` | `postgres::reader::PgReader` | `PgCursor` | yes | application event log in Postgres |
| SQS | `sqs::writer::SqsWriter` | `sqs::reader::SqsReader` | `NoCursor` | no | queue delivery, visibility timeout redelivery |
| Kafka | `kafka::writer::KafkaWriter` | `kafka::reader::KafkaReader` | `KafkaCursor` | no | stream delivery, consumer group commits |

SQL readers are source readers over append-only event tables. Their ackers track
source cursor progress for the active stream, but durable consumer progress lives
in `SqliteCheckpointStore` or `PgCheckpointStore` and is added with
`CheckpointReader`.

SQS and Kafka use their native delivery semantics. Their ackers batch deletes or
offset commits through `BatchedAcker`.

## Subscriptions and Filters

Each backend owns its subscription/config type because each backend has different
positioning and protocol concerns.

Common subscription/config types:

- `memory::reader::MemorySubscription { limit }`
- `sqlite::reader::SqliteSubscription { start, filter, batch_size, limit }`
- `postgres::reader::PgSubscription { start, filter, batch_size, limit }`
- `sqs::reader_config::SqsReaderConfig`
- `kafka::reader_config::KafkaReaderConfig`

Shared event filtering lives in `eventuary::io::filter::EventFilter`:

```rust
use eventuary::OrganizationId;
use eventuary::io::filter::EventFilter;
use eventuary::sqlite::reader::SqliteSubscription;

let subscription = SqliteSubscription {
    filter: EventFilter::for_organization(OrganizationId::new("acme")?),
    ..SqliteSubscription::default()
};
```

SQLite and PostgreSQL push supported filters into their SQL queries. Other
backends can be composed with `FilteredReader` when filtering should happen after
reading.

## Checkpointing SQL Readers

SQL source readers can replay by cursor, but they do not persist consumer
progress by themselves. Compose them with `CheckpointReader` and the matching SQL
checkpoint store.

```rust
use eventuary::{Result, StartFrom};
use eventuary::io::{ConsumerGroupId, Reader, StreamId};
use eventuary::io::reader::{
    CheckpointReader, CheckpointScope, CheckpointSubscription,
};
use eventuary::sqlite::checkpoint_store::{
    SqliteCheckpointStore, SqliteCheckpointStoreConfig,
};
use eventuary::sqlite::database::SqliteDatabase;
use eventuary::sqlite::reader::{SqliteCursor, SqliteReader, SqliteReaderConfig, SqliteSubscription};

async fn checkpointed_sqlite() -> Result<()> {
    let db = SqliteDatabase::open_in_memory()?;
    let source = SqliteReader::new(db.conn(), SqliteReaderConfig::default());
    let store: SqliteCheckpointStore<SqliteCursor> =
        SqliteCheckpointStore::new(db.conn(), SqliteCheckpointStoreConfig::default());
    let reader = CheckpointReader::new(source, store);

    let scope = CheckpointScope::new(
        ConsumerGroupId::new("orders-projection")?,
        StreamId::new("orders")?,
    );

    let subscription = CheckpointSubscription::new(
        SqliteSubscription {
            start: StartFrom::Earliest,
            ..SqliteSubscription::default()
        },
        scope,
    );

    let _stream = reader.read(subscription).await?;
    Ok(())
}
```

Checkpoint keys are scoped by `(consumer_group_id, stream_id, cursor_id)`. A raw
SQL reader uses the global cursor id. A partitioned reader uses one cursor id per
logical partition.

`CheckpointReader` commits only contiguous delivered progress per cursor id. On
`ack`, it acks the inner message first and then commits the cursor. On `nack`, it
leaves the checkpoint untouched.

## Partitioned Readers

`PartitionedReader` is an in-process lane scheduler over any reader. It routes
events into logical lanes using `Event::partition(count)`, which uses the event
key when present and the event id otherwise.

Use source mode for source-cursor readers such as PostgreSQL and SQLite:

```rust
use eventuary::{Result, StartFrom};
use eventuary::io::{ConsumerGroupId, Reader, StreamId};
use eventuary::io::reader::{
    CheckpointReader, CheckpointScope, CheckpointSubscription, PartitionedCursor,
    PartitionedReader, PartitionedReaderConfig, PartitionedSubscription,
};
use eventuary::sqlite::checkpoint_store::{
    SqliteCheckpointStore, SqliteCheckpointStoreConfig,
};
use eventuary::sqlite::database::SqliteDatabase;
use eventuary::sqlite::reader::{SqliteCursor, SqliteReader, SqliteReaderConfig, SqliteSubscription};

async fn partitioned_checkpointed_sqlite() -> Result<()> {
    let db = SqliteDatabase::open_in_memory()?;
    let source = SqliteReader::new(db.conn(), SqliteReaderConfig::default());
    let partitioned = PartitionedReader::source(source, PartitionedReaderConfig::default());
    let store: SqliteCheckpointStore<PartitionedCursor<SqliteCursor>> =
        SqliteCheckpointStore::new(db.conn(), SqliteCheckpointStoreConfig::default());
    let reader = CheckpointReader::new(partitioned, store);

    let scope = CheckpointScope::new(
        ConsumerGroupId::new("orders-projection")?,
        StreamId::new("orders")?,
    );

    let subscription = CheckpointSubscription::new(
        PartitionedSubscription::new(SqliteSubscription {
            start: StartFrom::Earliest,
            ..SqliteSubscription::default()
        }),
        scope,
    );

    let _stream = reader.read(subscription).await?;
    Ok(())
}
```

Use delivery mode for destructive-ack brokers such as SQS and Kafka:

```rust
use eventuary::io::reader::{PartitionedReader, PartitionedReaderConfig};

let partitioned = PartitionedReader::delivery(reader, PartitionedReaderConfig::default());
```

Source mode acks the inner source when a message is accepted into a lane and
handles downstream `nack` by requeueing the event in memory. Delivery mode keeps
the inner acker until downstream `ack`/`nack`, preserving broker semantics.

## Reader Wrappers

Cross-cutting reader behavior lives in `eventuary::io::reader` and can be
composed around backend readers.

| Wrapper | Purpose |
|---------|---------|
| `FilteredReader` | Drop events that do not match a filter |
| `BatchReader` | Emit batched messages |
| `ConcurrencyLimitReader` | Bound active downstream messages |
| `DedupeReader` | Skip duplicate events using a dedupe store |
| `InspectReader` | Run hooks around reader delivery/ack activity |
| `MapReader` | Transform events infallibly |
| `TryMapReader` | Transform events fallibly |
| `MergeReader` | Merge multiple readers into one stream |
| `RateLimitReader` | Throttle delivery rate |
| `RecoverReader` | Recover from transient stream errors |
| `ReplayThenLiveReader` | Replay a historical source, then switch to live delivery |
| `TimeoutReader` | Add timeout behavior around delivery/ack paths |
| `WatermarkReader` | Track processed watermarks |
| `WindowReader` | Window event delivery |
| `PartitionedReader` | Route events into deterministic logical lanes |
| `CheckpointReader` | Persist cursor progress on ack |

Wrappers are generic over the `Reader` trait, so they are backend-independent.

## Consumer Driver and Retry

`BackgroundConsumer` connects a reader, subscription, and handler:

```rust
use eventuary::{Event, Result};
use eventuary::io::Handler;

struct LogHandler;

impl Handler for LogHandler {
    fn id(&self) -> &str {
        "log-handler"
    }

    async fn handle(&self, event: &Event) -> Result<()> {
        tracing::info!(topic = %event.topic(), "received event");
        Ok(())
    }
}
```

The consumer driver:

- opens the reader stream,
- calls `Handler::handle(&Event)`,
- acks on success,
- nacks on handler error or timeout,
- supports bounded concurrency and graceful shutdown.

For retries and dead-letter routing, wrap a handler with `RetryHandler`:

```rust
use eventuary::io::handler::{DeadLetterWriter, DefaultRetryPolicy, RetryConfig, RetryHandler};

let handler = RetryHandler::new(
    LogHandler,
    DefaultRetryPolicy,
    DeadLetterWriter::new(dead_letter_writer),
    RetryConfig::default(),
);
```

Dead-letter events are written to `<original_topic>.dead_letter` with failure
metadata and the original event payload preserved.

## SQL Relations and Migrations

SQLite and PostgreSQL create an append-only events table and a consumer offsets
table by default. Relation names are validated before being rendered into SQL.

PostgreSQL supports schema-qualified relation names:

```rust
use eventuary::postgres::database::{PgDatabase, PgDatabaseConfig};
use eventuary::postgres::relation::PgRelationName;

let config = PgDatabaseConfig {
    events_relation: PgRelationName::new("eventuary.events")?,
    offsets_relation: PgRelationName::new("eventuary.consumer_offsets")?,
    ..PgDatabaseConfig::default()
};

let db = PgDatabase::connect_with_config(database_url, config).await?;
```

The database modules also expose migration metadata and rendered schema SQL for
projects that manage migrations outside Eventuary.

## Backend Notes

### memory

- Uses a `tokio::mpsc` channel per writer/reader pair.
- Emits `Message<NoopAcker, NoCursor>`.
- No persistence, replay, checkpointing, or filtering in the backend.
- Best suited for tests, examples, and simple in-process flows.

### sqlite

- Uses `rusqlite` with the bundled SQLite feature.
- Runs blocking database work in `tokio::task::spawn_blocking`.
- `SqliteReader` polls the configured events relation by sequence.
- `SqliteCursorAcker` tracks active stream cursor progress in memory.
- `SqliteCheckpointStore<C>` persists full cursor JSON keyed by consumer group,
  stream id, and cursor id.
- Relation names are validated through `SqliteRelationName`.

### postgres

- Uses `sqlx` with `runtime-tokio` and Postgres support.
- `PgReader` polls the configured events relation by sequence.
- `PgCursorAcker` tracks active stream cursor progress in memory.
- `PgCheckpointStore<C>` persists full cursor JSON keyed by consumer group,
  stream id, and cursor id.
- `PgDatabaseConfig::with_schema` can place default tables in a schema.
- Integration tests use `postgres:18-alpine` through `testcontainers`.

### sqs

- Uses `aws-sdk-sqs` long polling.
- `SqsWriter` serializes events as `SerializedEvent` JSON.
- `SqsReader` emits messages with `BatchedAcker<String>` receipt-handle tokens.
- Ack deletes messages in batches; nack changes visibility timeout to zero.
- SQS supports only `StartFrom::Latest` in reader config and has no historical
  replay cursor.

### kafka

- Uses `rdkafka` with `cmake-build` and `tokio` features.
- `KafkaWriter` publishes `SerializedEvent` JSON with event key as the Kafka key.
- `KafkaReader` emits messages with `BatchedAcker<KafkaOffsetToken>`.
- Ack commits the highest observed offset per partition through the consumer
  group; nack leaves offsets uncommitted.
- Reader config uses Kafka topic names in `kafka_topics` and supports
  `StartFrom::Earliest`, `Latest`, and `Timestamp`.

## Error Model

Eventuary uses one error enum and a shared result alias:

```rust
use eventuary::{Error, Result};
```

Error variants include validation errors (`InvalidTopic`, `InvalidNamespace`,
`InvalidOrganization`, `InvalidPayload`, `InvalidEventKey`,
`InvalidConsumerGroupId`, `InvalidStartFrom`, `InvalidCursor`), serialization
errors, backend store errors, timeouts, and configuration errors.

Backends map driver/protocol failures to `Error::Store` or `Error::Config`.
Domain validation stays in the `Invalid*` variants.

## Development

Run workspace checks with Cargo directly:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --lib
cargo test -p eventuary-memory
cargo test -p eventuary-sqlite
```

Verify umbrella feature combinations that users may depend on:

```bash
cargo check -p eventuary --no-default-features
cargo check -p eventuary --no-default-features --features "memory"
cargo check -p eventuary --no-default-features --features "sqlite"
cargo check -p eventuary --no-default-features --features "postgres,kafka"
cargo test -p eventuary --doc --all-features
```

Container-backed integration tests use `testcontainers`. With rootless Podman:

```bash
export DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock
export TESTCONTAINERS_RYUK_DISABLED=true

cargo test -p eventuary-postgres -- --test-threads=1
cargo test -p eventuary-sqs -- --test-threads=1
cargo test -p eventuary-kafka -- --test-threads=1
```

Kafka builds `librdkafka` through the `cmake-build` feature and requires system
build dependencies such as `cmake`, OpenSSL, libcurl, SASL, zlib, and
`pkg-config`.

## Releasing

Publishing to crates.io is gated on a GitHub release, a version tag, or a manual
workflow dispatch. Pushes to `main` do not publish.

The publish workflow verifies that the tag matches `workspace.package.version`,
runs formatting, clippy, and unit tests, then publishes in dependency order:

1. `eventuary-core`
2. backend crates and `eventuary-conformance`
3. `eventuary` umbrella crate

Release procedure:

```bash
# 1. Bump workspace.package.version in Cargo.toml.
# 2. Commit and push the version bump.
# 3. Tag the release.
git tag v0.1.0-alpha.1
git push origin v0.1.0-alpha.1
# 4. The publish workflow runs automatically.
```

A `CARGO_REGISTRY_TOKEN` repository secret is required for publishing.

## License

MIT — see [LICENSE](LICENSE).
