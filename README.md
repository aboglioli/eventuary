# Eventuary

Eventuary is a Rust event toolkit for logs, queues, streams, routing, replay,
checkpointing, and acknowledgements across multiple backends.

It provides a small typed event model, async IO traits, composable reader
wrappers, and optional backend implementations for in-memory channels, the
local filesystem, SQLite, PostgreSQL, AWS (SQS, SNS), and Apache Kafka. Everything intended for application use is
available through the `eventuary` umbrella crate, with backends enabled by Cargo
features.

> **Status:** `0.2.0` stable; `0.3.0-rc.3` pre-release.

Eventuary is a library you embed in your application. It is not a server, broker,
daemon, or transport runtime.

## Install

Most applications should depend on the umbrella crate and enable the backend
features they need. No backend is enabled by default.

```toml
[dependencies]
eventuary = { version = "0.3.0-rc.3", features = ["postgres"] }
```

| Feature | Module | Backend crate |
|---------|--------|---------------|
| `memory` | `eventuary::memory` | [`eventuary-memory`](crates/eventuary-memory) |
| `fs` | `eventuary::fs` | [`eventuary-fs`](crates/eventuary-fs) |
| `sqlite` | `eventuary::sqlite` | [`eventuary-sqlite`](crates/eventuary-sqlite) |
| `postgres` | `eventuary::postgres` | [`eventuary-postgres`](crates/eventuary-postgres) |
| `aws` | `eventuary::aws` | [`eventuary-aws`](crates/eventuary-aws) |
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
use eventuary::postgres::checkpoint::{PgCheckpointStore, PgCheckpointStoreConfig};
use eventuary::postgres::database::{PgDatabase, PgDatabaseConfig};
use eventuary::postgres::relation::PgRelationName;
```

### Direct sub-crate use

Advanced users can depend on sub-crates directly:

| Crate | When to depend directly |
|-------|-------------------------|
| [`eventuary-core`](crates/eventuary-core) | Building a custom backend or using only the event model and IO traits |
| [`eventuary-memory`](crates/eventuary-memory), etc. | Pinning or publishing against a backend crate independently |

Applications should normally use the umbrella crate.

## Workspace Layout

```text
crates/
├── eventuary/              # umbrella crate; re-exports core + feature-gated backends
├── eventuary-core/         # event model, serialization, IO traits, reader wrappers, consumer driver
├── eventuary-memory/       # tokio::mpsc backend for tests/dev/single-process use
├── eventuary-fs/           # partitioned, segmented append-only log on plain files
├── eventuary-sqlite/       # rusqlite append-only event log + checkpoint store
├── eventuary-postgres/     # sqlx/Postgres append-only event log + checkpoint store
├── eventuary-aws/          # AWS backends: SQS writer/reader with batched delete acks, SNS writer
├── eventuary-kafka/        # rdkafka writer/reader with batched offset commits
└── eventuary-conformance/  # internal conformance scaffold, not published
```

Layering rules:

- `eventuary-core` does not depend on any backend crate.
- Backend crates depend on `eventuary-core`, not on the umbrella crate.
- The umbrella crate contains no original implementation code; it only re-exports
  `eventuary-core` and optional backend crates.
- Backend crate roots expose role modules only. Import concrete backend implementations through their role module paths (`reader`, `writer`, `checkpoint`, `coordinator`, etc.). This keeps the backend API consistent across memory, SQLite, PostgreSQL, AWS, and Kafka. `eventuary-aws` nests one extra segment because it hosts several AWS services (`aws::sqs::reader`, `aws::sns::writer`).

## Core Event Model

Every event has two identities:

- `id`: unique occurrence id, generated as UUID v7.
- `key`: required routing/stream identity, shared by related events such as all events for `invoice-123`.

`key` is not unique. Use it for partitioning, Kafka record keys, aggregate/entity routing, and deterministic lane assignment. Use `id` for dedupe and event occurrence identity.

```rust
use eventuary::{Event, Payload};

let event = Event::builder(
    "acme",
    "/billing",
    "invoice.created",
    "invoice-123",
    Payload::from_json(&serde_json::json!({ "amount": 100 }))?,
)?
.build()?;
```

Important model types:

- `Event` — immutable event record with UUID v7 `id`, tenant organization,
  namespace, topic, key, payload, metadata, timestamp, and version. Tracing and
  lineage values live in `metadata`; see
  [Correlation and causation](#correlation-and-causation).
- `Payload` — JSON, plain text, or binary content. Internally it uses
  `bytes::Bytes`, so cloning a payload is cheap and does not copy the byte
  buffer.
- `Topic` — dot-separated lowercase topic name with digits, `_`, and `-`.
- `Namespace` — slash-rooted hierarchy such as `/`, `/billing`, or
  `/billing/invoices`.
- `OrganizationId` — tenant scope; `_platform` is reserved for platform-wide
  events.
- `EventKey` — required non-empty routing/stream key. `EventKey` is the stable
  entity key commonly used by the partition resolver pipeline. Use
  `eventuary::partition::EventKeyPartitionKeyResolver` with
  `Fnv1a64PartitionHasher` for deterministic FNV-1a partition routing.
- `Metadata` — validated string key/value metadata.
- `SerializedEvent` — stable wire format used by every backend.
- `FieldMap<V>` — reusable validated key/value storage backing `Metadata` and
  `Context`. Keys must be non-empty, must not have leading or trailing
  whitespace, and must not contain newlines.

### Correlation and causation

`Event` carries no `correlation_id`, `causation_id`, or `parent_id` field.
Lineage is application context, so it belongs in `metadata`:

```rust
use eventuary::{Event, Metadata, Payload};

let parent = Event::create("acme", "/billing", "invoice.created", "invoice-123",
    Payload::from_string("created"))?;

let event = Event::builder(
    "acme",
    "/billing",
    "invoice.paid",
    "invoice-123",
    Payload::from_string("paid"),
)?
.metadata(
    Metadata::new()
        .with("correlation_id", "billing-run-7")?
        .with("causation_id", parent.id().to_string())?,
)
.build()?;
```

Why metadata rather than dedicated fields:

- **`EventFilter` already matches metadata subsets**, so a metadata-carried
  correlation id is filterable through the public API. Dedicated fields were
  not, in any backend.
- **Every backend persists metadata already** — JSONB in PostgreSQL, JSON text
  in SQLite, the `metadata` object in the `SerializedEvent` wire format used by
  fs, SQS, SNS, and Kafka. No schema change is needed to adopt a convention.
- **Applications disagree about what lineage is.** A W3C `traceparent`, an HTTP
  request id, a saga id, and a job id all compete for the same slot, and several
  applications need more than one at a time. An open map holds all of them; a
  fixed trio holds none of them well.

Eventuary defines no reserved metadata keys and does not populate lineage for
you. Pick key names once and apply them consistently across producers.

Both SQL backends can index a metadata key when lineage becomes a query path:

```sql
-- PostgreSQL (metadata is JSONB)
CREATE INDEX idx_events_correlation ON events ((metadata->>'correlation_id'));
CREATE INDEX idx_events_metadata ON events USING GIN (metadata jsonb_path_ops);

-- SQLite (metadata is JSON text; JSON1 ships with the bundled build)
CREATE INDEX idx_events_correlation
ON events (json_extract(metadata, '$.correlation_id'));
```

Those indexes serve your own queries. Eventuary's readers scan by `sequence` and
apply `EventFilter` after the fetch, so they do not plan against them.

### Context Values

`Context` is a serializable bag of typed fields used by `NackContext` and other
contextual flows. Build it with `Context::new(message)` and chain `.with(key,
value)` calls; values are converted into `ContextValue` through `From` impls for
strings, booleans, integer and floating-point primitives, `serde_json::Value`,
and `Error`.

```rust
use eventuary::Context;

let context = Context::new("handler failed")
    .with("handler_id", "billing")?
    .with("attempt", 3_u64)?
    .with("retryable", true)?;
```

`ContextValue` is a tagged enum (`String`, `Bool`, `U64`, `I64`, `F64`,
`Error`, `Json`). A `serde_json::Value` is preserved as `ContextValue::Json`
regardless of its shape, and non-finite `f64` values (`NaN`, `±∞`) are silently
dropped so the context stays JSON-safe. `ContextError` stores a stable
`(kind, message)` pair derived from `Error`, so errors round-trip through JSON
without losing their variant tag.

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

`Acker::nack()` is the basic compatibility path and leaves the rejection cause
implicit. `Acker::nack_with(NackContext)` is the additive contextual path: it
carries a `NackReason` (`HandlerError`, `HandlerTimeout`, `ProcessingRejected`,
`DeliveryExpired`, `RouteFailed`, `Unknown`) plus a serializable `Context` with
typed fields. Built-in helpers cover the common cases; the default `nack_with`
impl falls back to `nack`, so existing ackers keep working unchanged.

```rust
use eventuary::Error;
use eventuary::io::acker::NackContext;

let nack_context = NackContext::handler_error(
    handler.id(),
    Error::Handler("payment declined".to_owned()),
)?;

msg.nack_with(nack_context).await?;
```

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

## Typed Payloads

`Event<P = Payload>` is generic over the payload type. The default `Payload`
is the canonical wire format used by every durable backend (SQL, SQS, Kafka).
For in-memory or in-process flows you can carry a typed `P` end-to-end and
avoid serialization round-trips.

Every IO trait defaults to `Payload` so existing code is unchanged. The same
traits accept a typed `P`:

```rust,ignore
pub trait Reader<P = Payload> { /* ... */ }
pub trait Writer<P = Payload> { /* ... */ }
pub trait Handler<P = Payload> { /* ... */ }
pub trait Filter<P = Payload>  { /* ... */ }
```

Reader/writer/handler wrappers (`MapReader`, `BatchWriter`, `FilteredHandler`,
`InspectReader`, `Multiplexer`, …) are generic in `P` and compose for any
custom payload type. The in-memory backend is the natural fit:

```rust,ignore
use eventuary::memory::reader::{MemoryReader, MemorySubscription};
use eventuary::memory::writer::MemoryWriter;
use eventuary::{Event, io::{Reader, Writer}};
use futures::StreamExt;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
struct OrderPlaced { order_id: String, total: u64 }

# tokio_test::block_on(async {
let (tx, rx) = mpsc::channel::<Event<OrderPlaced>>(64);
let writer = MemoryWriter::new(tx);
let reader = MemoryReader::new(rx);

let event = Event::create(
    "acme",
    "/orders",
    "order.placed",
    "order-1",
    OrderPlaced { order_id: "o-1".into(), total: 42 },
).unwrap();
writer.write(&event).await.unwrap();

let mut stream = reader.read(MemorySubscription { limit: Some(1) }).await.unwrap();
let msg = stream.next().await.unwrap().unwrap();
assert_eq!(msg.event().payload().order_id, "o-1");
# });
```

### Bridging typed and durable

Durable backends always speak `Payload`. To carry a typed `P` through a
pipeline whose source or sink is durable, bridge at the edges:

- `DecodeReader<R, C, P>` wraps a durable `Reader<Payload>` and exposes a
  typed `Reader<P>`. Use `ReaderTypedExt::decode(codec)` with a
  `PayloadCodec<P>` when only the payload needs decoding, or
  `ReaderTypedExt::decode_event(codec)` with an `EventCodec<P>` when decoding
  needs access to the full event envelope. Decode failures are routed per
  `DecodeErrorDisposition` (`AckInner`, `NackInner`, or `Surface`); the
  default is `AckInner` so a poison event does not stall source progress.
- `EncodeWriter<W, C, P>` wraps a durable `Writer<Payload>` and exposes a
  typed `Writer<P>`. Use `WriterTypedExt::encode(codec)` with a
  `PayloadCodec<P>` for payload-only encoding, or
  `WriterTypedExt::encode_event(codec)` with an `EventCodec<P>` when encoding
  needs full-event context.

```rust,ignore
use eventuary::io::reader::ReaderTypedExt;
use eventuary::io::writer::WriterTypedExt;
use eventuary::{JsonPayloadCodec, PayloadEventCodec};

let typed_reader = source_reader.decode::<OrderPlaced, _>(JsonPayloadCodec);
let typed_writer = durable_writer.encode::<OrderPlaced, _>(JsonPayloadCodec);

let event_codec = PayloadEventCodec::new(JsonPayloadCodec);
let typed_reader = source_reader.decode_event::<OrderPlaced, _>(event_codec.clone());
let typed_writer = durable_writer.encode_event::<OrderPlaced, _>(event_codec);
```

`SerializedEvent`, the SQL writers/readers, SQS, Kafka, and durable
`BufferStore` / `DedupeStore` / `MultiplexerStore` implementations all
remain `Payload`-bound by design — the wire boundary is the only place
serialization happens.

## In-Memory Backend Example

```toml
[dependencies]
eventuary = { version = "0.3.0-rc.3", features = ["memory"] }
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
        "order-1",
        Payload::from_json(&serde_json::json!({ "total": 42 }))?,
    )?
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
| filesystem | `fs::writer::FsWriter` | `fs::reader::FsReader` | `FsCursor` | yes | durable local event log with no server |
| SQLite | `sqlite::writer::SqliteWriter` | `sqlite::reader::SqliteReader` | `SqliteCursor` | yes | embedded durable event log |
| PostgreSQL | `postgres::writer::PgWriter` | `postgres::reader::PgReader` | `PgCursor` | yes | application event log in Postgres |
| SQS | `aws::sqs::writer::SqsWriter` | `aws::sqs::reader::SqsReader` | `NoCursor` | no | queue delivery, visibility timeout redelivery |
| SNS | `aws::sns::writer::SnsWriter` | — (publish-only) | — | no | topic fanout to subscribed queues |
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
- `fs::reader::FsSubscription { start, partition_starts, stop_at, filter, partitions, batch_size, limit }`
- `sqlite::reader::SqliteSubscription { start, stop_at, filter, partitions, batch_size, limit }`
- `postgres::reader::PgSubscription { start, stop_at, filter, partitions, batch_size, limit }`
- `aws::sqs::reader::SqsReaderConfig`
- `kafka::reader::KafkaReaderConfig`

`StartFrom<C>` controls where replay begins. SQL subscriptions also support
`StopAt<C>` so callers can choose live tailing (`StopAt::Never`, the default), a
finite snapshot ending at the current log end (`StopAt::CurrentEnd`), or a
finite range ending at a specific cursor (`StopAt::Cursor(cursor)`).

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

SQL subscriptions also expose `partitions: PartitionSelection`:

- `All` (default) — fetch every partition.
- `One(Partition)` — fetch a single partition; set via
  `subscription.with_partition(p)` (the convenience default on the
  `PartitionableSubscription` trait).
- `Many(PartitionGroup)` — fetch a validated set of partitions sharing the
  same `partition_count`; set via `subscription.with_partitions(group)`
  (the primary `PartitionableSubscription` trait method that
  `CoordinatedReader` uses to push its owned set into the inner reader).
  PostgreSQL emits `partition_id = ANY($::int[])`, SQLite emits
  `partition_id IN (?, ?, ...)`, so the reader serves all selected lanes
  in one SQL round-trip per poll instead of one round-trip per lane.

```rust
use std::num::NonZeroU32;
use eventuary::partition::{Partition, PartitionGroup};
use eventuary::sqlite::reader::SqliteSubscription;

let count = NonZeroU32::new(8).unwrap();
let group = PartitionGroup::new(vec![
    Partition::new(1, count)?,
    Partition::new(4, count)?,
])?;

let subscription = SqliteSubscription::default().with_partitions(group);
```

`CoordinatedReader` uses `Many` internally — it composes
`PartitionedReader::source_from_cursor` over a shared inner reader
filtered by `PartitionSelection::Many(owned)`. The same pairing is
available to callers that compose a partition-aware reader outside the
coordinator path, and it is the natural fit for future backends whose
native protocol assigns partition sets directly (e.g., Kafka).

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
use eventuary::sqlite::checkpoint::{
    SqliteCheckpointStore, SqliteCheckpointStoreConfig,
};
use eventuary::sqlite::database::SqliteDatabase;
use eventuary::sqlite::reader::{SqliteCursor, SqliteReader, SqliteReaderConfig, SqliteSubscription};

async fn checkpointed_sqlite() -> Result<()> {
    let db = SqliteDatabase::open_in_memory()?;
    let source = SqliteReader::connect(db.conn(), SqliteReaderConfig::default())?;
    let store: SqliteCheckpointStore<SqliteCursor> =
        SqliteCheckpointStore::connect(db.conn(), SqliteCheckpointStoreConfig::default())?;
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

Default partitioning hashes `Event::key()` with FNV-1a. Because `key` is required, all default reader and writer partitioning paths are deterministic by stream/entity identity. Use custom `PartitionKeyResolver` implementations when partitioning by organization, topic, namespace, metadata, or a composite key.

`PartitionedReader` is an in-process lane scheduler over any reader. By default,
it routes events by resolving `event.key()` with `EventKeyPartitionKeyResolver`
and hashing with `Fnv1a64PartitionHasher`. Customize routing through
`PartitionedReaderConfig::with_router(resolver, hasher)`.

Use source mode for source-cursor readers such as PostgreSQL and SQLite:

```rust
use eventuary::{Result, StartFrom};
use eventuary::io::{ConsumerGroupId, Reader, StreamId};
use eventuary::io::reader::{
    CheckpointReader, CheckpointScope, CheckpointSubscription, PartitionedCursor,
    PartitionedReader, PartitionedReaderConfig, PartitionedSubscription,
};
use eventuary::sqlite::checkpoint::{
    SqliteCheckpointStore, SqliteCheckpointStoreConfig,
};
use eventuary::sqlite::database::SqliteDatabase;
use eventuary::sqlite::reader::{SqliteCursor, SqliteReader, SqliteReaderConfig, SqliteSubscription};

async fn partitioned_checkpointed_sqlite() -> Result<()> {
    let db = SqliteDatabase::open_in_memory()?;
    let source = SqliteReader::connect(db.conn(), SqliteReaderConfig::default())?;
    let partitioned = PartitionedReader::source(source, PartitionedReaderConfig::default());
    let store: SqliteCheckpointStore<PartitionedCursor<SqliteCursor>> =
        SqliteCheckpointStore::connect(db.conn(), SqliteCheckpointStoreConfig::default())?;
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

When the inner source is **partition-aware** (its cursor implements
`HasPartition` — true for `PgReader` and `SqliteReader`), prefer
`PartitionedReader::source_from_cursor`. It routes each event onto the
lane matching the persisted `partition_id` carried in the cursor rather
than re-hashing `event.key()`, so the writer's `PartitionKeyResolver` +
`PartitionHasher` choice is honored automatically. `CoordinatedReader`
uses this constructor internally. Use the original `source` constructor
for non-partition-aware sources (in-memory channels, transformed streams
without persisted partition columns) where the lane index must be
derived from the event key.

`HasPartition` is the capability trait for any cursor type carrying a
resolved `Partition`. It is implemented by `PgCursor`, `SqliteCursor`,
and `PartitionedCursor<C: HasPartition>` (via blanket impl), and it
bounds `PartitionedReader::source_from_cursor` so only partition-aware
sources can be wired into cursor-driven lane routing.

## Multi-Instance Coordinated Readers

### SQL partitioning prerequisite

SQL partitioned and coordinated readers filter on persisted `partition_count`
and `partition_id` columns. The default SQL writers leave those columns `NULL`,
so coordinated readers only see events written with inline partitioning enabled
or rows that have been backfilled.

Enable inline partitioning when the rows will be consumed by
`CoordinatedReader` or by `PartitionedReader::source_from_cursor`.

Raw SQL readers can read rows whose partition columns are `NULL` when
`PartitionSelection::All` is used. Those rows receive a synthetic cursor
partition `(id = 0, count = 1)` so default writer/default reader flows keep
working without partitioning.

Partition-filtered reads (`PartitionSelection::One` / `Many`),
`PartitionedReader::source_from_cursor`, and `CoordinatedReader` require real
partition columns. Use inline writer partitioning or run the backend partition
backfill before using those flows.

> **Footgun:** Switching from a default writer (`NULL` partition columns) to
> inline partitioning over an existing event log without running
> `PgPartitionBackfill` / `SqlitePartitionBackfill` produces a mixed log where
> raw `PartitionSelection::All` reads split checkpoint state between the
> synthetic `(0, 1)` cursor and real `(id, count)` cursors. `CheckpointReader`
> tracks progress per cursor id, so the two lanes resume independently and can
> redeliver or skip events at the cutover. Always backfill before enabling
> inline partitioning on a non-empty log.

PostgreSQL example:

```rust,ignore
use std::num::NonZeroU32;

use eventuary::partition::{EventKeyPartitionKeyResolver, Fnv1a64PartitionHasher};
use eventuary::postgres::writer::{PgPartitioningConfig, PgWriter, PgWriterConfig};

let partition_count = NonZeroU32::new(1024).unwrap();
let writer = PgWriter::new_with_config(
    pool.clone(),
    PgWriterConfig {
        partitioning: PgPartitioningConfig::inline(
            partition_count,
            EventKeyPartitionKeyResolver,
            Fnv1a64PartitionHasher,
        ),
        ..PgWriterConfig::default()
    },
);
```

SQLite uses the same shape with `SqlitePartitioningConfig` and `SqliteWriter`.

For existing rows, run `PgPartitionBackfill` (PostgreSQL) or
`SqlitePartitionBackfill` (SQLite) with the same resolver, hasher, and
partition count before starting coordinated readers.


`PartitionedReader` distributes partitions inside one process. To distribute
partitions across multiple service instances sharing the same logical consumer
group, compose a partition-aware source reader with `CoordinatedReader<R, Coord>`
and a `PartitionCoordinator<C>`.

`CoordinatedReader` provides Kafka-like consumer-group semantics over a SQL log:

- heartbeats this instance into `event_stream_consumers` for the
  `(consumer_group_id, stream_id)` scope,
- claims free, expired, or released partitions from `event_stream_partitions`,
- spawns one inner `PartitionedReader::source_from_cursor` over a single
  shared `PgReader` filtered by `PartitionSelection::Many(owned)`, with
  one in-memory lane per owned partition; lanes are merged into a single
  stream,
- renews leases in a shared background loop with bounded jitter,
- writes monotonic checkpoints fenced by `(owner_id, generation)`;
  a stale owner's checkpoint update affects zero rows and surfaces
  `Error::OwnershipLost`,
- rebalances when new instances heartbeat in or owners disappear,
- releases owned partitions gracefully when the stream is dropped or shut down.

The backend crates expose this as thin type aliases over the core generic:

| Backend | Type aliases | Coordinator |
|---------|--------------|-------------|
| PostgreSQL | `PgCoordinatedReader`, `PgCoordinatedAcker`, `PgCoordinatedCursor` | `PgPartitionCoordinator` |
| SQLite | `SqliteCoordinatedReader`, `SqliteCoordinatedAcker`, `SqliteCoordinatedCursor` | `SqlitePartitionCoordinator` |
| memory | — (use the core generic directly) | `MemoryPartitionCoordinator` |

```rust,ignore
use std::num::NonZeroU32;
use std::sync::Arc;

use eventuary::StartFrom;
use eventuary::io::{ConsumerGroupId, OwnerId, StreamId};
use eventuary::io::reader::{CheckpointScope, CoordinatedReaderConfig, CoordinatedSubscription};
use eventuary::postgres::reader::PgCoordinatedReader;
use eventuary::postgres::coordinator::PgPartitionCoordinator;
use eventuary::postgres::reader::{PgReader, PgReaderConfig, PgSubscription};

let source = PgReader::new(pool.clone(), PgReaderConfig::default());
let coordinator = Arc::new(PgPartitionCoordinator::new(pool, Default::default()));

let reader = PgCoordinatedReader::new(
    source,
    Arc::clone(&coordinator),
    OwnerId::generate(),
    CoordinatedReaderConfig::default(),
);

let subscription = CoordinatedSubscription {
    inner: PgSubscription::default(),
    scope: CheckpointScope::new(
        ConsumerGroupId::new("orders-projection-v1")?,
        StreamId::new("orders-events")?,
    ),
    partition_count: NonZeroU32::new(1024).unwrap(),
    start: StartFrom::Earliest,
};
```

Identity for coordinated state is `(consumer_group_id, stream_id, partition_id)`,
so multiple consumer groups can independently consume the same `stream_id` with
their own ownership and checkpoint progress.

`partition_slack` defaults to `0` so rebalances converge toward
`ceil(partitions / live_consumers)` ownership. Increase it only when you
intentionally prefer fewer releases during churn over immediate even
distribution.

Checkpoint flush is configurable via
`CheckpointFlushPolicy { max_pending_acks, max_pending_interval }`.
The default is per-ack flush (`max_pending_acks = 1`,
`max_pending_interval = Duration::ZERO`) for parity with prior versions;
raise either threshold to reduce write amplification at the cost of
widening the redelivery window on owner crash. The pending-ack buffer is
per-partition; fencing semantics are unchanged.

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
| `MergeReader` | Merge two readers with fair, left-priority, or weighted scheduling |
| `RateLimitReader` | Throttle delivery rate |
| `RecoverReader` | Recover from transient stream errors |
| `ReplayThenLiveReader` | Replay a historical source, then switch to live delivery |
| `TimeoutReader` | Add timeout behavior around delivery/ack paths |
| `WatermarkReader` | Track processed watermarks |
| `WindowReader` | Window event delivery |
| `PartitionedReader` | Route events into deterministic logical lanes |
| `CheckpointReader` | Persist cursor progress on ack |
| `CoordinatedReader` | Multi-instance partition ownership over SQL (heartbeat, lease, fenced checkpoint, rebalance) |
| `OutcomeRouterReader` | Route delivered, acked, and/or nacked events to writers while preserving message lifecycle semantics |

Wrappers are generic over the `Reader` trait, so they are backend-independent.

`OutcomeRouterReader` exposes three constructors — `on_ack`, `on_nack`, and
`on_delivery` — plus the `with_ack_writer` / `with_nack_writer` /
`with_delivery_writer` builders for combining them, and
`with_nack_disposition` / `with_delivery_disposition` for the routing policy.
`NackDisposition::NackInner` preserves redelivery on routed nacks;
`AckInnerAfterRoute` moves the event to the side flow after a successful route
write. `DeliveryDisposition::RequireRoute` (default) nacks the inner with
`NackReason::RouteFailed` and surfaces the error when the delivery route fails;
`DeliveryDisposition::BestEffort` swallows the route error and still delivers
the original message downstream.

## Writer Wrappers

Writer-side flow behavior lives in `eventuary::io::writer` and composes with any
backend writer. Wrappers are deliberately small: mapping, filtering, fanout,
retry, timeout, inspection, and batching are separate responsibilities.

| Wrapper | Purpose |
|---------|---------|
| `MapWriter` | Transform `&Event` into a new event before writing |
| `TryMapWriter` | Fallibly transform `&Event` into a new event before writing |
| `FlatMapWriter` | Derive one or many events from `&Event` and forward them through `write_all` |
| `TryFlatMapWriter` | Fallibly derive one or many events from `&Event` and forward them through `write_all` |
| `FilteredWriter` | Skip non-matching events |
| `FanoutWriter` | Write the same event to multiple writers concurrently |
| `RetryWriter` | Retry failed writes with exponential backoff |
| `TimeoutWriter` | Bound write and batch-write latency |
| `InspectWriter` | Run hooks around write attempts, successes, and errors |
| `BatchWriter` | Batch concurrent writes by size or wait time and flush through `write_all` |

These wrappers are useful with `OutcomeRouterReader`: route nacked events to a
writer, map them into a dead-letter envelope with `TryMapWriter`, fan them out to
multiple destinations with `FanoutWriter`, and protect the route with
`RetryWriter` or `TimeoutWriter`.

```rust,ignore
use eventuary::io::reader::{NackDisposition, OutcomeRouterReader};
use eventuary::io::writer::{FanoutWriter, TryMapWriter};

let dead_letter_writer = TryMapWriter::new(writer, |event: &eventuary::Event| {
    build_dead_letter_event(event)
});

let failed_writer = FanoutWriter::new(vec![
    audit_writer.into_arced(),
    dead_letter_writer.into_arced(),
])?;

let reader = OutcomeRouterReader::on_nack(source_reader, failed_writer)
    .with_nack_disposition(NackDisposition::AckInnerAfterRoute);
```

## Consumer Driver and Handler Wrappers

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

Before nacking, `BackgroundConsumer` builds a `NackContext` and calls
`Message::nack_with`. Handler failures use `NackReason::HandlerError` with the
handler id and source error; per-call timeouts use `NackReason::HandlerTimeout`.
`TimeoutReader` likewise nacks expired messages with `NackReason::DeliveryExpired`.
`InspectReader` exposes the contextual path through
`InspectHooks::on_nack_with(&Event, &NackContext)`; the default impl delegates
to `on_nack` so existing hook implementations keep working.

Handler wrappers live in `eventuary::io::handler` and compose around any
handler:

| Wrapper | Purpose |
|---------|---------|
| `FilteredHandler` | Skip events that do not match a filter |
| `TimeoutHandler` | Bound handler execution latency |
| `InspectHandler` | Run hooks around handler start, success, and error |
| `RateLimitHandler` | Throttle handler executions |
| `RetryHandler` | Retry handler failures and optionally dead-letter final failures |

For retries and dead-letter routing, wrap a handler with `RetryHandler`:

```rust
use eventuary::io::handler::{DeadLetterWriter, DefaultRetryPolicy, RetryConfig, RetryHandler};

let handler = RetryHandler::new(
    LogHandler,
    DefaultRetryPolicy,
    RetryConfig::default(),
    DeadLetterWriter::new(dead_letter_writer),
);
```

Dead-letter events are written to `<original_topic>.dead_letter` with failure
details and the original event payload preserved. The original event's
`metadata` is copied onto the dead-letter event, so a metadata-carried
correlation id survives into the dead-letter topic.

## Handler Multiplexing

`Multiplexer` is a handler that routes one event to every matching subscriber:

```rust,ignore
use std::num::NonZeroUsize;

use eventuary::io::filter::EventFilter;
use eventuary::io::handler::Multiplexer;
use eventuary::memory::multiplexer::MemoryMultiplexerStore;

let multiplexer = Multiplexer::builder()
    .route("orders-projection", EventFilter::default(), orders_projection)?
    .route("audit-log", EventFilter::default(), audit_handler)?
    .store(MemoryMultiplexerStore::with_capacity(NonZeroUsize::new(10_000).unwrap()))
    .build()?;
```

The backend message still has one lifecycle. SQS has one receipt handle, Kafka
has one offset in one consumer group, and SQL readers have one delivery cursor.
The source is acked only after every matching subscriber succeeds. If any
subscriber fails, the source is nacked and may redeliver the same event.

`SubscriberId` is the durable identity of a subscriber. It is distinct from
`Handler::id`: a handler's id is its observability label, while a subscriber id
is the storage key for `(event_id, subscriber_id)` completion. Choose
subscriber ids that are globally unique across every multiplexer that touches
the same store; if you need isolation between multiplexers, namespace the
subscriber id (`orders:projection`, `inventory:projection`).

`Multiplexer` is generic over its store. The default `NoMultiplexerStore` is
a zero-sized no-op: handlers run on every redelivery and must be idempotent.
Calling `.store(...)` switches the builder to a real implementation
(`MemoryMultiplexerStore` or a backend-provided durable store); successful
`(event_id, subscriber_id)` pairs are then marked completed and skipped on
redelivery.

Concurrency defaults to `1` (preserves route declaration order). Values greater
than `1` run matching subscribers concurrently without order guarantees — use
`1` when one subscriber must run before another for the same event.

For fully independent subscriber progress, use backend-native isolation: one
SQL `CheckpointScope` per subscriber, one Kafka consumer group per subscriber,
or one SQS queue per subscriber.

## Parallelism and Concurrency

Two stacks provide per-partition ordering with cross-partition parallel
handler execution. Pick based on deployment topology.

### Single-instance stack

```
PgReader (1 SQL query, fetches all rows)
  └── PartitionedReader::source (N in-memory lanes, one in-flight each)
        └── CheckpointReader (per-lane durable cursor)
              └── BackgroundConsumer::spawn(handler, concurrency=N)
```

- 1 SQL query per poll cycle regardless of partition count.
- `PartitionedReader` routes events into N lanes in memory by hashing
  `event.key()`.  Each lane is FIFO — per-key ordering is preserved.
- `CheckpointReader` persists per-lane `PartitionedCursor<PgCursor>` so the
  consumer resumes from the last acked event in each lane.
- `BackgroundConsumer` processes up to `concurrency` events in parallel.
  Set `concurrency` to match the lane count for full parallelism.
- **No partition columns needed on the writer.** Partition assignment is
  computed from the event key in memory.

### Multi-instance stack

```
CoordinatedReader (lease + heartbeat + fenced checkpoint)
  └── PartitionedReader::source_from_cursor (N in-memory lanes, one in-flight each)
        └── PgReader { selection: PartitionSelection::Many(owned) } (1 SQL query)
              └── BackgroundConsumer::spawn(handler, concurrency=N)
```

- **1 SQL query per poll cycle**, regardless of how many partitions the
  owner holds. The inner `PgReader` uses `PartitionSelection::Many(owned)`
  to fetch all owned partitions in a single
  `partition_id = ANY($1::int[])` query.
- `CoordinatedReader` replaces `CheckpointReader` for the multi-instance
  case: it owns lease coordination, partition routing (via SQL filter on
  the inner `PgReader`), and fenced durable checkpointing via the
  `PartitionCoordinator` trait. Internally it composes
  `PartitionedReader::source_from_cursor` over the inner `PgReader` for
  lane scheduling — applications do not stack these readers manually.
- Lane routing trusts the persisted `partition_id` column via the
  `HasPartition` capability trait on the cursor, so the writer's
  `PartitionKeyResolver` + `PartitionHasher` choice is honored by
  construction.
- Lease-based ownership with heartbeat, renewal, and rebalance. The
  owned set is propagated into the inner `PgReader`'s
  `PartitionSelection::Many` on every rebalance so partitions
  redistribute across instances dynamically.
- `CoordinatedReader<R, Coord>` is generic over both the source reader
  and the coordinator, so backends can be mixed at the composition layer
  (e.g., a `PgReader` source paired with a future Redis coordinator).
- **Requires partition columns in the DB** — the writer must use
  `PgPartitioningConfig::Inline` (or run `PgPartitionBackfill` /
  `SqlitePartitionBackfill` on existing rows).

### How concurrency works in both stacks

Both stacks deliver **at most one in-flight event per partition**. The
mechanism differs:

| Stack | Flow control | Nack redelivery |
|---|---|---|
| Single-instance | `PartitionedReader` lane scheduler over a non-partition-aware source: one in-flight per lane, ack releases slot, lane index from key hash | Requeue at lane head |
| Multi-instance | `PartitionedReader::source_from_cursor` lane scheduler over a partition-aware `PgReader`: one in-flight per lane, ack releases slot, lane index from persisted partition column | Requeue at lane head; durable resume from fenced checkpoint on owner crash |

In both cases the next event from partition X cannot enter the stream
until the current one is acked. `BackgroundConsumer` processes events from
different partitions concurrently (`for_each_concurrent`), but no two
events from the same partition are ever in-flight simultaneously.

Both stacks share the **same** lane-scheduler implementation. The only
differences are the data source (a non-partition-aware reader hashed on
`event.key()` vs a partition-aware `PgReader` routed by the persisted
`partition_id`) and where durable progress is persisted
(`consumer_offsets` vs `event_stream_partitions`).

### Partition count guidelines

| Scenario | Recommended |
|---|---|
| Single-instance stack | 8–128 (one SQL query regardless of lane count) |
| Multi-instance stack, moderate scale (≤10 instances, ≤10K events/sec) | 64–256 |
| Multi-instance stack, high scale (>10 instances or >10K events/sec) | 256–1024 |
| Strong per-entity ordering with many distinct entities | up to 1024+ — choose by hash-collision tolerance |

`partition_count` is a `NonZeroU32`, so there is no practical upper bound
from the type system; pick by domain rather than infrastructure budget.

More partitions than concurrent handlers does not increase throughput —
the bottleneck remains handler concurrency, not SQL. Both stacks now
issue one SQL query per poll regardless of partition count. Use
`PartitionedReader` for single-process parallelism; use
`CoordinatedReader` when you need ownership distribution across
instances. Partition count is decoupled from the number of consumer
instances and from infra cost — pick it by domain (entity-routing
granularity, ordering parallelism).

## SQL Relations and Migrations

Relation names are validated before being rendered into SQL. PostgreSQL supports
schema-qualified relation names (e.g. `PgRelationName::new("eventuary.events")?`).

Schema setup is component-owned. Use `connect` when Eventuary should create a
component's schema. Use `new` / `new_with_config` when migrations are managed
externally. `PgDatabase` and `SqliteDatabase` only open a pool/connection; they
do not create Eventuary tables. Applications that use multiple SQL components
should call each component's `connect` or `prepare_schema` explicitly.

```rust
// PostgreSQL
let writer = PgWriter::connect(pool.clone(), PgWriterConfig::default()).await?;
let dedupe = PgDedupeStore::connect(pool.clone(), PgDedupeStoreConfig::default()).await?;

// SQLite
let writer = SqliteWriter::connect(conn.clone(), SqliteWriterConfig::default())?;
let dedupe = SqliteDedupeStore::connect(conn.clone(), SqliteDedupeStoreConfig::default())?;
```

To place tables in a non-default schema, pass a `PgRelationName` in the
component config (e.g.
`PgWriterConfig { events_relation: PgRelationName::new("eventuary.events")?, .. }`).
Each component's `schema_sql(config)` method returns the rendered DDL for
projects that manage migrations outside Eventuary.

`PgDatabaseConfig` exposes only `{ max_connections: u32 }`:

```rust
use eventuary::postgres::database::{PgDatabase, PgDatabaseConfig};

let db = PgDatabase::connect_with_config(database_url, PgDatabaseConfig {
    max_connections: 10,
}).await?;
```

## Backend Notes

### memory

- Uses a `tokio::mpsc` channel per writer/reader pair.
- Emits `Message<NoopAcker, NoCursor>`.
- No persistence, replay, checkpointing, or filtering in the backend.
- Best suited for tests, examples, and simple in-process flows.

### fs

- Stores each partition as a directory of size-rolled segments under a log root.
  No server, no driver, and no C dependency.
- Segment files are JSON lines carrying a flat `offset` field, so a log stays
  readable with `cat`, `grep` and `jq`.
- Sparse offset and time indexes let a read seek near its target instead of
  scanning from the start of the segment.
- `FsWriter` takes an exclusive advisory lock on every partition it opens, so a
  log has **one producer process**. `FsWriter::open_partitions_subset` splits
  production across processes by partition.
- `FsCursor` is `{ partition, offset }` and implements `HasPartition`, so
  `PartitionedReader::source_from_cursor` and `FsCoordinatedReader` route by the
  persisted partition rather than re-hashing the key.
- `FsCheckpointStore<C>` writes one JSON file per cursor id.
  `FsPartitionCoordinator` claims partitions under `(owner_id, generation)`
  fenced leases with monotonic checkpoints, matching the SQL coordinators.
- `SyncPolicy` defaults to one fsync per megabyte appended. Use
  `SyncPolicy::Always` when no acknowledged event may ever be lost.
- `RetentionPolicy` selects whole segments to drop by age or total size, never
  the active one. Reclaiming is caller-driven: `FsWriter::enforce_retention`
  does the deleting and nothing calls it for you, so schedule it from the
  application. A consumer whose checkpoint falls behind the retained range gets
  `Error::InvalidCursor` rather than silently skipping the deleted events.
- A record that no longer decodes into an `Event` ends that partition's stream,
  naming its partition and offset, and keeps doing so until someone intervenes,
  because skipping it would lose an event the log still holds. Segments are JSON
  lines, so the repair is to correct or delete that line.
- `FsBufferStore` numbers entries from a counter held in the process that opened
  it, so one buffer directory belongs to one process.
- Coordination uses advisory file locks, so it is single-node. Putting the log
  on a shared network filesystem does not make it multi-host.
- Integration tests need no containers.

### sqlite

- Uses `rusqlite` with the bundled SQLite feature.
- Runs blocking database work in `tokio::task::spawn_blocking`.
- `SqliteReader` polls the configured events relation by sequence.
- `SqliteCursorAcker` tracks active stream cursor progress in memory.
- `SqliteCheckpointStore<C>` persists full cursor JSON keyed by consumer group,
  stream id, and cursor id.
- Relation names are validated through `SqliteRelationName`.
- `SqliteDatabase::open` / `open_in_memory` only open a connection and do not
  create Eventuary tables.

### postgres

- Uses `sqlx` with `runtime-tokio` and Postgres support.
- `PgReader` polls the configured events relation by sequence.
- `PgCursorAcker` tracks active stream cursor progress in memory.
- `PgCheckpointStore<C>` persists full cursor JSON keyed by consumer group,
  stream id, and cursor id.
- `PgDatabaseConfig` is `{ max_connections: u32 }`. `PgDatabase::connect` only
  opens a pool and does not create Eventuary tables.
- Integration tests use `postgres:18-alpine` through `testcontainers`.

### aws

`eventuary-aws` hosts every AWS backend. Its modules nest by service first and
role second (`aws::sqs::reader`, `aws::sns::writer`) because the crate covers
more than one service. It supersedes the retired `eventuary-sqs` crate.

**SQS**

- Uses `aws-sdk-sqs` long polling.
- `SqsWriter` serializes events as `SerializedEvent` JSON.
- `SqsQueueType` selects a standard or FIFO queue. FIFO sends carry
  `MessageGroupId` from `event.key()` and `MessageDeduplicationId` from
  `event.id()`; a FIFO queue rejects a send without a group id.
- `SqsReaderConfig` takes the queue type too: FIFO polls carry a
  `ReceiveRequestAttemptId` so a retried receive returns the same messages
  instead of stalling the message group until the visibility timeout expires.
- `SqsReader` emits messages with `BatchedAcker<String>` receipt-handle tokens.
- Ack deletes messages in batches; nack changes visibility timeout to zero.
- SQS has no historical replay cursor, so `SqsReaderConfig` carries no start
  position and no consumer group id; the queue URL is the consumer identity.
- `SqsSubscription::validate` runs on every read, so a hand-built subscription
  is checked against the SQS protocol bounds, not just the reader's own config.
- A message the reader cannot decode into an `Event` is logged at warn with its
  message id and the decode error, then deleted.

**SNS**

- Uses `aws-sdk-sns` `Publish` / `PublishBatch` (10 entries, 256 KB per request).
- `SnsWriter` publishes the same `SerializedEvent` JSON wire format, so a
  subscribed queue can be consumed with `SqsReader`.
- **Publish-only: there is no `SnsReader`.** SNS has no receive API. The
  canonical topology is SNS → SQS fanout — publish once, subscribe one queue
  per consumer, read each with `SqsReader`.
- `SnsTopicType` selects a standard or FIFO topic, mapping FIFO requirements
  onto event identity exactly as `SqsQueueType` does.

> **Footgun:** queues subscribed to an SNS topic must have
> `RawMessageDelivery` enabled. Without it SNS wraps the body in a notification
> envelope (`{"Type":"Notification","Message":"..."}`) that `SqsReader` cannot
> decode as a `SerializedEvent`, so every event is treated as a poison record
> and silently ack-skipped — the queue drains and nothing reaches the handler.

```rust,ignore
use eventuary::aws::sns::writer::SnsWriter;
use eventuary::aws::sqs::reader::{SqsReader, SqsReaderConfig};

let writer = SnsWriter::new(sns_client, "arn:aws:sns:us-east-1:123456789012:orders");
writer.write(&event).await?;

let reader = SqsReader::new(sqs_client, SqsReaderConfig::defaults_for(queue_url))?;
```

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
cargo test -p eventuary-fs
cargo test -p eventuary-sqlite
```

Verify umbrella feature combinations that users may depend on:

```bash
cargo check -p eventuary --no-default-features
cargo check -p eventuary --no-default-features --features "memory"
cargo check -p eventuary --no-default-features --features "fs"
cargo check -p eventuary --no-default-features --features "sqlite"
cargo check -p eventuary --no-default-features --features "postgres,kafka"
cargo test -p eventuary --doc --all-features
```

Container-backed integration tests use `testcontainers`. With rootless Podman:

```bash
export DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock
export TESTCONTAINERS_RYUK_DISABLED=true

cargo test -p eventuary-postgres -- --test-threads=1
cargo test -p eventuary-aws -- --test-threads=1
cargo test -p eventuary-kafka -- --test-threads=1
```

Kafka builds `librdkafka` through the `cmake-build` feature and requires system
build dependencies such as `cmake`, OpenSSL, libcurl, SASL, zlib, and
`pkg-config`.

## Releasing

Publishing to crates.io is gated on a published GitHub Release or a manual
workflow dispatch. Pushes to `main` and tag pushes do not publish.

The publish workflow verifies that the tag matches `workspace.package.version`,
runs formatting, clippy, and unit tests, then publishes in dependency order:

1. `eventuary-core`
2. backend crates
3. `eventuary` umbrella crate

Release procedure:

```bash
# 1. Bump the version in the root Cargo.toml. Both places must match:
#    - workspace.package.version
#    - every workspace.dependencies pin for an eventuary crate
#    A pin left behind fails the publish partway through, after the earlier
#    crates are already on crates.io and immutable.
# 2. Run `cargo check` so Cargo.lock records the new version.
# 3. Commit and push the version bump.
# 4. Create and publish a GitHub Release targeting main.
#    Use tag v0.3.0-rc.3 and title v0.3.0-rc.3.
#    For a pre-release, tick "Set as a pre-release"; the workflow listens on
#    `published`, which fires for pre-releases too.
# 5. The publish workflow runs automatically from the release event.
```

Pre-release versions (`-alpha.N`, `-beta.N`, `-rc.N`) follow SemVer: dot-separated
identifiers of `[0-9A-Za-z-]`, no leading zeros on numeric parts. Cargo will not
resolve a pre-release from a plain requirement, so `eventuary = "0.3.0"` never
selects `0.3.0-rc.3` — testers must ask for the exact version, and the
workspace pins above must carry the same suffix.

A `CARGO_REGISTRY_TOKEN` repository secret is required for publishing.

## License

MIT — see [LICENSE](LICENSE).
