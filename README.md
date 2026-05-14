# Eventuary

Eventuary is a Rust event toolkit for logs, queues, streams, routing, replay,
and acknowledgements across multiple backends.

A small core event model and async IO traits, plus optional backend
implementations for in-memory, SQLite, PostgreSQL, AWS SQS, and Apache Kafka.
Everything ships behind a single umbrella crate `eventuary` with backends
gated by Cargo features.

> **Status:** Alpha (`0.1.0-alpha.0`). API may change before `0.1.0`.

## Install

Most consumers should depend on the `eventuary` umbrella crate and pick
backends via Cargo features. No backend is enabled by default:

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

The umbrella re-exports `eventuary-core` at its root (so
`eventuary::Event`, `eventuary::Writer`, etc. work directly) and re-exports
each backend crate as a feature-gated module (`eventuary::memory::InmemReader`,
`eventuary::sqlite::SqliteReader`, …).

### Direct sub-crate use

The workspace also publishes the individual crates separately for advanced
use cases — for example, writing a custom backend without pulling the
umbrella, or wiring backend conformance tests directly:

| Crate | When to depend directly |
|-------|------------------------|
| [`eventuary-core`](crates/eventuary-core) | Building a new backend; want only the event model + IO traits |
| [`eventuary-conformance`](crates/eventuary-conformance) | Dev-dep for backend authors; reusable cursor-reader conformance cases are being rebuilt |
| [`eventuary-memory`](crates/eventuary-memory) etc. | Pinning a backend's version independently of the umbrella |

Typical applications should stick to the umbrella `eventuary` crate.

## Core Model

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
.build();
```

- `Event` — immutable record with `id` (UUID v7), `organization`, `namespace`,
  `topic`, `payload`, `metadata`, `timestamp`, `version`, and optional `key`,
  `parent_id`, `correlation_id`, `causation_id`.
- `Payload` — JSON, plain text, or binary content.
- `Topic` — dot-separated, lowercase/digit/`_`/`-`.
- `Namespace` — slash-rooted hierarchy: `/`, `/billing`, `/billing/invoices`.
- `OrganizationId` — tenant scope; `_platform` sentinel for cross-tenant.
- `EventKey` — non-empty flexible identifier (≤1024 chars) used for optional
  event keys, correlation IDs, and causation IDs. Exposes a `partition`
  method using FNV-1a for deterministic partition routing.
- `Metadata` — validated key/value pairs.
- `SerializedEvent` — wire format with `to_json_value` / `from_json_value` /
  `to_json_string` / `from_json_str` / `from_json_slice` helpers used by
  every backend.

## Async IO

```rust
use eventuary::{Event, Payload, Writer};

async fn emit<W: Writer>(writer: &W, event: &Event) -> eventuary::Result<()> {
    writer.write(event).await
}
```

Native async traits with `impl Future` return types — no `async-trait`. A dyn
bridge (`DynWriter`, `BoxWriter`, `ArcWriter` and same for `Reader`,
`Handler`, `Acker`) is provided for runtime composition and DI.

```rust
use eventuary::{BoxWriter, Event, Writer};

async fn emit_boxed(writer: &BoxWriter, event: &Event) -> eventuary::Result<()> {
    writer.write(event).await
}
```

## In-Memory Backend Example

```toml
[dependencies]
eventuary = { version = "0.1.0-alpha.0", features = ["memory"] }
```

```rust
use eventuary::io::EventFilter;
use eventuary::memory::{InmemReader, InmemWriter, MemorySubscription};
use eventuary::{Event, OrganizationId, Payload, Reader, Writer};
use tokio::sync::mpsc;

let (tx, rx) = mpsc::channel(100);
let writer = InmemWriter::new(tx);
let reader = InmemReader::new(rx);

let event = Event::builder(
    "acme",
    "/orders",
    "order.placed",
    Payload::from_json(&serde_json::json!({ "total": 42 }))?,
)?
.key("order-1")?
.build();

writer.write(&event).await?;

let subscription = MemorySubscription {
    filter: EventFilter::for_organization(OrganizationId::new("acme")?),
    limit: None,
};
let mut stream = reader.read(subscription).await?;
```

## Cursor + Checkpoint Composition

Backend readers deliver `Message<Acker, Cursor>`. `Event` remains immutable
and cursor-free; delivery state lives only in the message envelope.

SQL readers (`PgReader`, `SqliteReader`) are source readers: they read the
events table and manage in-memory cursor ack/nack. They do not persist
consumer progress. Durable progress is handled by `CheckpointReader` plus a
backend `CheckpointStore`.

```rust
use eventuary::io::checkpoint::{CheckpointScope, StreamId};
use eventuary::io::readers::{CheckpointReader, CheckpointSubscription};
use eventuary::sqlite::{
    SqliteCheckpointStore, SqliteCheckpointStoreConfig, SqliteReader,
    SqliteReaderConfig, SqliteSubscription,
};
use eventuary::{ConsumerGroupId, Reader, StartFrom};

let source = SqliteReader::new(db.conn(), SqliteReaderConfig::default());
let store = SqliteCheckpointStore::new(db.conn(), SqliteCheckpointStoreConfig::default());
let checkpointed = CheckpointReader::new(source, store);

let scope = CheckpointScope::new(
    ConsumerGroupId::new("orders-projection")?,
    StreamId::new("orders")?,
);
let mut stream = checkpointed
    .read(CheckpointSubscription::new(
        SqliteSubscription {
            start: StartFrom::Earliest,
            ..SqliteSubscription::default()
        },
        scope,
    ))
    .await?;
```

`PartitionedReader` is an in-process lane scheduler. It wraps any reader,
routes events into logical lanes using deterministic event-key partitioning,
acks the inner source after accepting a message into a lane, and exposes one
merged stream. Downstream `ack` clears the lane in-flight slot; downstream
`nack` requeues the same event at the front of that lane.

```rust
use eventuary::io::checkpoint::{CheckpointScope, StreamId};
use eventuary::io::readers::{
    CheckpointReader, CheckpointSubscription, PartitionedReader,
    PartitionedReaderConfig, PartitionedSubscription,
};
use eventuary::sqlite::{
    SqliteCheckpointStore, SqliteCheckpointStoreConfig, SqliteReader,
    SqliteReaderConfig, SqliteSubscription,
};
use eventuary::{ConsumerGroupId, Reader, StartFrom};

let scope = CheckpointScope::new(
    ConsumerGroupId::new("orders-projection")?,
    StreamId::new("orders")?,
);
let source = SqliteReader::new(db.conn(), SqliteReaderConfig::default());
let partitioned = PartitionedReader::new(source, PartitionedReaderConfig::default());
let checkpoint_store = SqliteCheckpointStore::new(db.conn(), SqliteCheckpointStoreConfig::default());
let checkpointed = CheckpointReader::new(partitioned, checkpoint_store);

let mut stream = checkpointed
    .read(CheckpointSubscription::new(
        PartitionedSubscription::new(SqliteSubscription {
            start: StartFrom::Earliest,
            ..SqliteSubscription::default()
        }),
        scope,
    ))
    .await?;
```

`CheckpointReader` commits checkpoints in contiguous delivered order per
progress point. `nack` does not advance the checkpoint. Every reader's
cursor implements the `Cursor` trait, which exposes `id() -> CursorId`.
`CursorId::Global` identifies readers with a single progress point;
`CursorId::Named(Arc<str>)` identifies cursors that mark independent
progress points (for example, `PartitionedCursor<C>` returns
`CursorId::Named("partition:{count}:{id}")`). `CheckpointReader` uses
`cursor.id()` to build the checkpoint key and to track per-progress-point
pending state.

SQL checkpoint stores persist the full inner cursor as JSON —
`PgCursor` / `SqliteCursor` for a raw source reader, or
`PartitionedCursor<...>` when composed under a `PartitionedReader`. The
`consumer_offsets` row is keyed by `(consumer_group_id, stream_id,
cursor_id)`, where `cursor_id` is `JSONB` on Postgres and `TEXT` on
SQLite.

On resume `CheckpointReader` seeds the inner subscription via
`StartableSubscription::with_start(StartFrom::After(min(stored_cursors)))`.
Each wrapper validates the parts of the resume cursor it understands. For
example, `PartitionedReader` inspects the resume `PartitionedCursor`'s embedded
`LogicalPartition` and returns `Error::InvalidCursor` if the stored partition
count differs from its configured count. `CheckpointResumePolicy` controls
whether `CheckpointReader` returns that error or retries with the
subscription's configured initial `StartFrom`.

## Configurable SQL Relations

Postgres and SQLite can use default table names or configured relation names:

```rust
use eventuary::postgres::{PgDatabase, PgDatabaseConfig, PgRelationName};

let config = PgDatabaseConfig {
    events_relation: PgRelationName::new("eventuary.events")?,
    offsets_relation: PgRelationName::new("eventuary.consumer_offsets")?,
    ..PgDatabaseConfig::default()
};
let db = PgDatabase::connect_with_config(database_url, config).await?;
```

Relation names are validated and rendered as quoted identifiers, including
schema-qualified names such as `eventuary.events`.

## Consumer Loop

```rust
use eventuary::{Event, Handler};

struct LogHandler;

impl Handler for LogHandler {
    fn id(&self) -> &str { "log-handler" }

    fn handle<'a>(&'a self, event: &'a Event)
        -> impl std::future::Future<Output = eventuary::Result<()>> + Send + 'a
    {
        async move {
            tracing::info!(topic = %event.topic(), "received");
            Ok(())
        }
    }
}
```

`Handler::handle` receives a borrowed `&Event` — the ack/nack envelope is
owned by the `Reader`'s `Message<A, C>` and managed by the consumer driver,
not by the handler.

`BackgroundConsumer` polls a `Reader`, applies an optional `Filter`, drives a
`Handler` per event, and acks on `Ok(())` / nacks on `Err(_)` or timeout.

For retries and dead-letter routing, wrap the handler in `RetryHandler`:

```rust
use eventuary::{DeadLetterWriter, RetryConfig, RetryHandler, DefaultRetryPolicy};

let handler = RetryHandler::new(
    LogHandler,
    DefaultRetryPolicy,
    DeadLetterWriter::new(dead_letter_writer),
    RetryConfig::default(),
);
```

## Backend Test Commands

The memory and SQLite crates have no external dependencies and run with
`cargo test`.

The Postgres, SQS, and Kafka crates use [`testcontainers`] for integration
tests. Set the rootless Podman socket if you use Podman:

```bash
export DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock
export TESTCONTAINERS_RYUK_DISABLED=true

cargo test -p eventuary-postgres -- --test-threads=1
cargo test -p eventuary-sqs -- --test-threads=1
cargo test -p eventuary-kafka -- --test-threads=1
```

The Kafka backend requires `cmake`, `libssl-dev`, `libcurl4-openssl-dev`,
`libsasl2-dev`, `zlib1g-dev`, and `pkg-config` to build `librdkafka`
(via the `cmake-build` rdkafka feature).

## Development

```bash
cargo fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --lib       # unit tests, no containers
cargo test -p eventuary-memory     # memory tests
cargo test -p eventuary-sqlite     # sqlite tests, no containers
cargo check -p eventuary --no-default-features --features "postgres,kafka"
```

## Releasing

Publishing to crates.io is gated on a release event — pushes to `main` never
publish. The workflow at `.github/workflows/publish.yml` runs only when:

1. A GitHub Release is published (`release: published`), or
2. A `vX.Y.Z` (or `vX.Y.Z-prerelease`) tag is pushed, or
3. A maintainer triggers `workflow_dispatch` (with optional dry-run input).

The workflow verifies the tag matches `workspace.package.version`, runs fmt /
clippy / unit tests, executes `cargo publish --dry-run` for `eventuary-core`,
then publishes the eight crates in three tiers:

1. `eventuary-core` (waits 60s for the crates.io index to settle)
2. `eventuary-memory`, `eventuary-sqlite`, `eventuary-postgres`,
   `eventuary-sqs`, `eventuary-kafka`, `eventuary-conformance` — all depend
   on `eventuary-core` only
3. `eventuary` (the umbrella, re-exports everything above)

### Release procedure

```bash
# 1. Bump version in root Cargo.toml under [workspace.package]
#    (all crates inherit via workspace.package.version)

# 2. Commit and push
git commit -am "chore: release v0.1.0-alpha.1"
git push origin main

# 3. Tag and push the tag (or create a Release in the GitHub UI,
#    which creates the tag for you)
git tag v0.1.0-alpha.1
git push origin v0.1.0-alpha.1

# 4. The Publish workflow runs automatically.
```

A `CARGO_REGISTRY_TOKEN` repository secret must be configured. Consider
migrating to crates.io [trusted publishing] once stabilized for the registry,
which removes the long-lived token.

[trusted publishing]: https://crates.io/docs/trusted-publishing

## License

MIT — see [LICENSE](LICENSE).
