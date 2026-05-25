# Eventuary

Eventuary is a Rust event toolkit for logs, queues, streams, routing, replay,
checkpointing, and acknowledgements across multiple backends.

It provides a small typed event model, async IO traits, composable reader
wrappers, and optional backend implementations for in-memory channels, SQLite,
PostgreSQL, AWS SQS, and Apache Kafka. Everything intended for application use is
available through the `eventuary` umbrella crate, with backends enabled by Cargo
features.

> **Status:** Alpha (`0.1.0-alpha.1`). API may change before `0.1.0`.

Eventuary is a library you embed in your application. It is not a server, broker,
daemon, or transport runtime.

## Install

Most applications should depend on the umbrella crate and enable the backend
features they need. No backend is enabled by default.

```toml
[dependencies]
eventuary = { version = "0.1.0-alpha.1", features = ["postgres"] }
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
└── eventuary-conformance/  # internal conformance scaffold, not published
```

Layering rules:

- `eventuary-core` does not depend on any backend crate.
- Backend crates depend on `eventuary-core`, not on the umbrella crate.
- The umbrella crate contains no original implementation code; it only re-exports
  `eventuary-core` and optional backend crates.
- Backend implementation types have canonical module paths such as
  `eventuary::sqlite::reader::SqliteReader`. Some backend crates also expose
  selected convenience re-exports from their crate root; prefer module paths
  when showing which role a type plays.

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
  causation IDs. `EventKey` is the stable entity key commonly used by the
  partition resolver pipeline. Use
  `eventuary::partition::EventKeyPartitionKeyResolver` with
  `Fnv1a64PartitionHasher` for deterministic FNV-1a partition routing.
- `Metadata` — validated string key/value metadata.
- `SerializedEvent` — stable wire format used by every backend.
- `FieldMap<V>` — reusable validated key/value storage backing `Metadata` and
  `Context`. Keys must be non-empty, must not have leading or trailing
  whitespace, and must not contain newlines.

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
  typed `Reader<P>` by decoding each event through a `PayloadCodec<P>` or
  `EventCodec<P>`. Decode failures are routed per
  `DecodeErrorDisposition` (`AckInner`, `NackInner`, or `Surface`); the
  default is `AckInner` so a poison event does not stall source progress.
- `EncodeWriter<W, C, P>` wraps a durable `Writer<Payload>` and exposes a
  typed `Writer<P>` by encoding outgoing typed events at the boundary.

```rust,ignore
use eventuary::io::reader::ReaderTypedExt;
use eventuary::io::writer::WriterTypedExt;
use eventuary::JsonPayloadCodec;

let typed_reader = source_reader.decode::<OrderPlaced, _>(JsonPayloadCodec);
let typed_writer = durable_writer.encode::<OrderPlaced, _>(JsonPayloadCodec);
```

`SerializedEvent`, the SQL writers/readers, SQS, Kafka, and durable
`BufferStore` / `DedupeStore` / `MultiplexerStore` implementations all
remain `Payload`-bound by design — the wire boundary is the only place
serialization happens.

## In-Memory Backend Example

```toml
[dependencies]
eventuary = { version = "0.1.0-alpha.1", features = ["memory"] }
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
- `sqlite::reader::SqliteSubscription { start, stop_at, filter, partitions, batch_size, limit }`
- `postgres::reader::PgSubscription { start, stop_at, filter, partitions, batch_size, limit }`
- `sqs::reader_config::SqsReaderConfig`
- `kafka::reader_config::KafkaReaderConfig`

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
  `subscription.with_partition(p)` (the `PartitionableSubscription` trait
  method that `CoordinatedReader` relies on).
- `Many(PartitionGroup)` — fetch a validated set of partitions sharing the
  same `partition_count`; set via `subscription.with_partitions(group)`.
  PostgreSQL emits `partition_id = ANY($::int[])`, SQLite emits
  `partition_id IN (?, ?, ...)`, so the reader serves all selected lanes
  in one SQL round-trip per poll instead of one round-trip per lane.

```rust
use std::num::NonZeroU16;
use eventuary::partition::{Partition, PartitionGroup};
use eventuary::sqlite::reader::SqliteSubscription;

let count = NonZeroU16::new(8).unwrap();
let group = PartitionGroup::new(vec![
    Partition::new(1, count)?,
    Partition::new(4, count)?,
])?;

let subscription = SqliteSubscription::default().with_partitions(group);
```

`CoordinatedReader` itself still uses one inner worker per claimed lease
(each with `PartitionSelection::One`), so `Many` is intended for callers
that compose a partition-aware reader outside the coordinator path or for
future backends whose native protocol assigns partition sets directly.

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
events into logical lanes using `PartitionRouteStrategy<P>`: the default
`EventCompatibility` strategy hashes the event key (or event id when no key
is set) with FNV-1a; the `ResolverHasher` strategy plugs in a custom
`PartitionKeyResolver<P>` + `PartitionHasher`.

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

## Multi-Instance Coordinated Readers

### SQL partitioning prerequisite

SQL partitioned and coordinated readers filter on persisted `partition_count`
and `partition_id` columns. The default SQL writers leave those columns `NULL`,
so coordinated readers only see events written with inline partitioning enabled
or rows that have been backfilled.

PostgreSQL example:

```rust,ignore
use std::num::NonZeroU16;

use eventuary::partition::{EventKeyPartitionKeyResolver, Fnv1a64PartitionHasher};
use eventuary::postgres::writer::{PgPartitioningConfig, PgWriter, PgWriterConfig};

let partition_count = NonZeroU16::new(1024).unwrap();
let writer = PgWriter::new_with_config(
    pool.clone(),
    PgWriterConfig {
        partitioning: PgPartitioningConfig::inline(
            partition_count,
            EventKeyPartitionKeyResolver::event_id_on_unkeyed(),
            Fnv1a64PartitionHasher,
        ),
        ..PgWriterConfig::default()
    },
);
```

SQLite uses the same shape with `SqlitePartitioningConfig` and `SqliteWriter`.

For existing PostgreSQL rows, run `PgPartitionBackfill` with the same resolver,
hasher, and partition count before starting coordinated readers. SQLite does not
yet expose a backfill helper; backfill SQLite rows during your migration with the
same FNV-1a resolver/hasher strategy used by the writer, or rewrite historical
events through an inline-partitioned writer in a controlled maintenance window.


`PartitionedReader` distributes partitions inside one process. To distribute
partitions across multiple service instances sharing the same logical consumer
group, compose a partition-aware source reader with `CoordinatedReader<R, Coord>`
and a `PartitionCoordinator<C>`.

`CoordinatedReader` provides Kafka-like consumer-group semantics over a SQL log:

- heartbeats this instance into `event_stream_consumers` for the
  `(consumer_group_id, stream_id)` scope,
- claims free, expired, or released partitions from `event_stream_partitions`,
- spawns one inner partition reader per owned lease, merges them into one stream,
- renews leases in a shared background loop with bounded jitter,
- writes monotonic checkpoints fenced by `(owner_id, generation)` on every ack;
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
use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::Duration;

use eventuary::StartFrom;
use eventuary::io::{ConsumerGroupId, OwnerId, StreamId};
use eventuary::io::reader::{CheckpointScope, CoordinatedReaderConfig, CoordinatedSubscription};
use eventuary::postgres::coordinated_reader::{PgCoordinatedReader, PgCoordinatedReaderConfig};
use eventuary::postgres::partition_coordinator::PgPartitionCoordinator;
use eventuary::postgres::reader::{PgReader, PgReaderConfig, PgSubscription};

let source = PgReader::new(pool.clone(), PgReaderConfig::default());
let coordinator = Arc::new(PgPartitionCoordinator::new(pool, Default::default()));

let reader = PgCoordinatedReader::new(
    source,
    Arc::clone(&coordinator),
    OwnerId::generate(),
    CoordinatedReaderConfig {
        partition_lease_duration: Duration::from_secs(60),
        partition_renew_interval: Duration::from_secs(15),
        consumer_lease_duration: Duration::from_secs(30),
        consumer_heartbeat_interval: Duration::from_secs(10),
        rebalance_interval: Duration::from_secs(10),
        partition_slack: 0,
    },
);

let subscription = CoordinatedSubscription {
    inner: PgSubscription::default(),
    scope: CheckpointScope::new(
        ConsumerGroupId::new("orders-projection-v1")?,
        StreamId::new("orders-events")?,
    ),
    partition_count: NonZeroU16::new(1024).unwrap(),
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

Checkpoints currently flush on every ack. High-throughput deployments should
plan for a future contiguous-progress flush buffer; the API does not yet expose
`checkpoint_flush_count` / `checkpoint_flush_interval`.

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
    DeadLetterWriter::new(dead_letter_writer),
    RetryConfig::default(),
);
```

Dead-letter events are written to `<original_topic>.dead_letter` with failure
metadata and the original event payload preserved.

## Handler Multiplexing

`Multiplexer` is a handler that routes one event to every matching subscriber:

```rust,ignore
use std::num::NonZeroUsize;

use eventuary::io::filter::EventFilter;
use eventuary::io::handler::Multiplexer;
use eventuary::memory::multiplexer_store::MemoryMultiplexerStore;

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

Publishing to crates.io is gated on a published GitHub Release or a manual
workflow dispatch. Pushes to `main` and tag pushes do not publish.

The publish workflow verifies that the tag matches `workspace.package.version`,
runs formatting, clippy, and unit tests, then publishes in dependency order:

1. `eventuary-core`
2. backend crates
3. `eventuary` umbrella crate

Release procedure:

```bash
# 1. Bump workspace.package.version in Cargo.toml.
# 2. Commit and push the version bump.
# 3. Create and publish a GitHub Release targeting main.
#    Use tag v0.1.0-alpha.1 and title v0.1.0-alpha.1.
# 4. The publish workflow runs automatically from the release event.
```

A `CARGO_REGISTRY_TOKEN` repository secret is required for publishing.

## License

MIT — see [LICENSE](LICENSE).
