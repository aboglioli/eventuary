# eventuary

Rust event toolkit: a typed event model and async IO traits with backend
implementations for in-memory, SQLite, PostgreSQL, AWS SQS, and Apache Kafka.

Eventuary started life as `orchy-events` inside the [orchy](https://github.com/aboglioli/orchy)
project and was extracted as a standalone library so other Rust projects can
reuse the same event-sourcing primitives and reader/writer abstractions.

> **Status:** Alpha (`0.1.0-alpha.0`). API may change before `0.1.0`.

## What Eventuary Provides

Three layers, organized as a multi-crate workspace with a **single umbrella
crate** that re-exports everything user-facing:

1. A **core event model** (`Event`, `Payload`, `Topic`, `Namespace`,
   `OrganizationId`, `EventKey`, `Metadata`) with validation and a wire
   format (`SerializedEvent`). Lives in `eventuary-core`.
2. **Async IO traits** (`Writer`, `Reader`, `Handler`, `Acker`, `Filter`,
   `Message<A, C>`) using native AFIT plus a dyn bridge (`BoxWriter`,
   `BoxReader<S, C>`, `BoxHandler`, …) for runtime composition. Also in
   `eventuary-core`. Readers expose a `Cursor` associated type and deliver
   `Message<Acker, Cursor>`; consumers resume by cursor or persist progress
   through a `CheckpointStore`. Lane fanout is provided by
   `PartitionedReader`.
3. **Backend crates** that implement those traits over real systems —
   `eventuary-memory`, `eventuary-sqlite`, `eventuary-postgres`,
   `eventuary-sqs`, `eventuary-kafka` — plus an `eventuary-conformance`
   crate for shared backend conformance types and reusable cases as they
   are rebuilt for cursor readers.

The top-level `eventuary` crate is an **umbrella facade**: it re-exports
`eventuary-core` at its root and exposes each backend behind a Cargo
feature flag. Typical consumers add a single line to their `Cargo.toml`:

```toml
eventuary = { version = "0.1.0-alpha.0", features = ["postgres"] }
```

…and import `eventuary::Event`, `eventuary::postgres::PgReader`, etc.
without ever depending on the sub-crates directly.

There is **no opinionated server, no broker, no transport**. Eventuary is
a library you embed.

## Workspace Layout

```
crates/
├── eventuary/              # umbrella facade — the published consumer-facing crate
│   ├── Cargo.toml          # features: memory, sqlite, postgres, sqs, kafka
│   └── src/lib.rs          # pub use eventuary_core::*; feature-gated `pub use <backend>` re-exports
│
├── eventuary-core/         # core: model, traits, serialization, retry/DLQ, consumer driver
│   └── src/
│       ├── event.rs        # Event aggregate, EventId, RestoreEvent
│       ├── event_key.rs    # EventKey + inherent partition() (FNV-1a)
│       ├── topic.rs        # Topic (dot-separated, lowercase/digits/_/-)
│       ├── namespace.rs    # Namespace (slash-rooted hierarchy)
│       ├── organization.rs # OrganizationId (tenant; "_platform" sentinel)
│       ├── consumer_group_id.rs # ConsumerGroupId (1..=64 chars)
│       ├── payload.rs      # Payload + ContentType (JSON / text / binary)
│       ├── metadata.rs     # Metadata key/value pairs
│       ├── collector.rs    # EventCollector (aggregate -> drain -> persist)
│       ├── partition.rs    # LogicalPartition, CursorPartition, CommitCursor, partition_for, fnv1a_u64
│       ├── snapshot.rs     # Snapshot + SnapshotEventId
│       ├── start_from.rs   # StartFrom<C> (Earliest / Latest / Timestamp / After)
│       ├── serialization.rs # SerializedEvent wire format
│       ├── error.rs        # Error enum, Result alias
│       └── io/
│           ├── writer.rs   # Writer trait + DynWriter / BoxWriter / ArcWriter
│           ├── reader.rs   # Reader trait (assoc Subscription/Acker/Cursor/Stream)
│           ├── handler.rs  # Handler + dyn bridges; handlers borrow &Event
│           ├── message.rs  # Message<A, C> (event + acker + cursor), NoCursor
│           ├── filters.rs  # Filter trait, EventFilter, AllFilter, TopicFilter, NamespacePrefixFilter
│           ├── checkpoint.rs # CheckpointStore trait, CheckpointKey/Scope, StreamId
│           ├── readers/
│           │   ├── partitioned_reader.rs # PartitionedReader lane scheduler + PartitionedCursor
│           │   └── checkpoint_reader.rs  # CheckpointReader + CheckpointAcker
│           ├── acker/
│           │   ├── noop.rs      # NoopAcker
│           │   ├── once.rs      # OnceAcker (single-shot wrapper)
│           │   ├── batched.rs   # BatchedAcker + AckBuffer + BatchFlusher
│           │   └── either.rs    # Re-exports Either for variant ackers
│           └── consumers/
│               ├── background.rs # BackgroundConsumer + ConsumerHandle
│               └── retry.rs      # RetryHandler, RetryPolicy, DeadLetterWriter
│
├── eventuary-memory/       # in-memory tokio::mpsc backend; NoopAcker + NoCursor
├── eventuary-sqlite/       # rusqlite source reader/writer + SqliteCheckpointStore
├── eventuary-postgres/     # sqlx Postgres source reader/writer + PgCheckpointStore
├── eventuary-sqs/          # aws-sdk-sqs, BatchedAcker via SqsFlusher + NoCursor
├── eventuary-kafka/        # rdkafka StreamConsumer, BatchedAcker via KafkaFlusher
└── eventuary-conformance/  # direct dev-dep crate for backend conformance scaffolding
```

### Layer Rules

| Layer | Can Import | Cannot Import |
|-------|-----------|---------------|
| `eventuary-core` | stdlib, serde, uuid, chrono, futures, tokio, either, base64 | any other eventuary crate |
| `eventuary-conformance` | `eventuary-core` + tokio + tracing + uuid | any backend crate (backends depend on it, not vice versa) |
| `eventuary-<backend>` | `eventuary-core` + its native driver (rusqlite / sqlx / aws-sdk-sqs / rdkafka) | any other backend crate; `eventuary-conformance` only in `[dev-dependencies]` |
| `eventuary` (umbrella) | `eventuary-core` + every backend crate (optional, feature-gated) | nothing else; the umbrella owns no code beyond re-exports |

Key invariants:

- **The umbrella has zero original code.** Its `lib.rs` is `pub use
  eventuary_core::*;` plus feature-gated `pub use eventuary_<backend> as
  <backend>;`. Any new type must land in `eventuary-core` or a backend
  crate, never in the umbrella.
- **Backends depend only on `eventuary-core`**, not on the umbrella.
  Otherwise a backend would pull in every sibling backend through the
  umbrella's optional deps.
- **The umbrella's features map 1:1 to backend crates.** Adding a backend
  means adding both a `eventuary-<x>` crate and a matching feature in the
  umbrella's `Cargo.toml`.

The conformance crate is exposed only as a **direct sub-crate**
(`eventuary-conformance`), not re-exported through the umbrella. Backend
authors add it as a `[dev-dependencies]` entry, alongside `eventuary-core`.
The reusable case suite is being rebuilt for the cursor-reader API; keep
backend-specific integration coverage in place while doing that work.

## Key Patterns

### Event Aggregate + Collector

Every domain aggregate owns an `EventCollector`. Every state mutation
collects a semantic event. The store/repository drains the collector and
hands the events to a `Writer`:

```
Aggregate.mutate() -> EventCollector.collect(event)
                    -> save() -> drain_events() -> Writer::write(event)
```

`Event` is immutable, validated at construction (`Event::create`), and
reconstructed via `Event::new(...)`.
UUID v7 is used for `EventId` so events are time-ordered.

### Constructor Convention

| Pattern | Purpose | Validates? |
|---------|---------|------------|
| `T::new(...)` | First-time construction (value objects, aggregates) | Yes |
| `Event::create(...)` | Domain creation that may emit events | Yes |
| `Event::new(...)` | Restoration from persisted state | Yes (prepares for future validation) |

`Event::new(...)` uses positional params matching the struct field layout. Never
direct-cast strings into value objects: use the constructor (`Topic::new`,
`Namespace::new`, etc.) so validation runs.

### Native Async Traits + Dyn Bridge

Core IO traits use AFIT (Arbitrary Function In Traits):

```rust
pub trait Writer: Send + Sync {
    fn write<'a>(&'a self, event: &'a Event) -> impl Future<Output = Result<()>> + Send + 'a;
}

pub trait Reader: Send + Sync {
    type Subscription: Send;
    type Acker: Acker;
    type Cursor: Send;
    type Stream: Stream<Item = Result<Message<Self::Acker, Self::Cursor>>> + Send;
    fn read(&self, subscription: Self::Subscription) -> impl Future<Output = Result<Self::Stream>> + Send;
}
```

For runtime composition / DI, every trait has a `Dyn*` sibling and
`Box*` / `Arc*` aliases:

| Static trait | Dyn trait | Boxed alias | Arc alias |
|--------------|-----------|-------------|-----------|
| `Writer` | `DynWriter` | `BoxWriter` | `ArcWriter` |
| `Reader` | `DynReader<S, C>` | `BoxReader<S, C>` | `ArcReader<S, C>` |
| `Handler` | `DynHandler` | `BoxHandler` | `ArcHandler` |
| `Acker` | `DynAcker` | `BoxAcker` | `ArcAcker` |
| `Filter` | — | `BoxFilter` | `ArcFilter` |

Use `WriterExt::into_boxed()` / `into_arced()`, `ReaderExt::into_boxed()` /
`into_arced()`, etc. to erase. `Box<dyn DynWriter>` still implements
`Writer`, so erased values can pass back through generic APIs.

`Reader::Subscription` is an associated type so each backend can specialize
(Kafka has a richer config than memory). `Reader::Cursor` is the matching
delivery-cursor type. SQL source readers expose `PgCursor` / `SqliteCursor`;
memory, SQS, and Kafka currently deliver `NoCursor` because their native ack
systems own replay/progress semantics.

### Subscription Model

Each backend defines its own subscription type. The shared filter shape
lives in `EventFilter` (organization, topic patterns, namespace pattern,
key set, metadata subset, `end_at`); positional/cursor concerns live on
the subscription itself.

- `MemorySubscription { filter, limit }`
- `SqliteSubscription { start: StartFrom<SqliteCursor>, filter, batch_size, limit }`
- `PgSubscription { start: StartFrom<PgCursor>, filter, batch_size, limit }`
- `KafkaSubscription { topics, consumer_group_id, start_from, event_filter, limit }`
- `SqsSubscription { queue_url, wait_time, visibility_timeout, max_messages, event_filter, limit }`

SQL backends push the org / topic / namespace / start-from filters into the
SQL query to prune at the DB; Kafka/SQS apply them in the consumer loop.

The `StartableSubscription<C>` trait abstracts `start: StartFrom<C>` so
`CheckpointReader` can call `subscription.with_start(StartFrom::After(cursor))`
when it resumes from a persisted checkpoint.

### Message + Acker

`Message<A: Acker, C = NoCursor>` pairs an `Event` with an `Acker` and a
`Cursor`. The handler receives the **borrowed event**
(`Handler::handle(&self, event: &Event)`); the ack/nack envelope and the
cursor stay with the consumer driver, not the handler.

Ack semantics are backend-specific:

| Backend | ack | nack |
|---------|-----|------|
| memory | no-op (`NoopAcker`) | no-op |
| sqlite/postgres source | advance the in-memory buffered-batch cursor; on nack, re-emit the same row at next poll | re-emit at next poll |
| sqs | `BatchedAcker` -> `SqsFlusher` -> `DeleteMessageBatch` | no-op; visibility timeout redelivers |
| kafka | `BatchedAcker` -> `KafkaFlusher` -> `consumer.commit` with highest contiguous offset per partition | no-op; offset left uncommitted, redelivered after rebalance/restart |

Durable consumer progress for SQL backends lives in
`PgCheckpointStore` / `SqliteCheckpointStore`, not in the reader. Compose
the source reader with `CheckpointReader` to commit progress on ack and
resume from the persisted cursor on next read.

### Batched Ack

`BatchedAcker<T>` + `BatchFlusher<Token = T>` + `AckBuffer<F>` implement
the standard "buffer up to N tokens or M seconds, then flush" pattern
used by SQS and Kafka. Spawned with `AckBuffer::spawn(flusher, config)`;
each `BatchedAcker` carries a token (receipt handle for SQS,
`(topic, partition, offset)` for Kafka) and a `Sender` into the buffer.

### Consumer Driver

`BackgroundConsumer<R, H>` ties a `Reader`, optional `Filter`, and
`Handler` together:

- Acks on `Handler::handle` success.
- Nacks on `Err(_)` or per-call timeout (`with_handler_timeout`).
- Bounded concurrency (`for_each_concurrent`).
- Cancellation via `CancellationToken`; in-flight handlers tracked by
  `TaskTracker` for graceful shutdown.

For retries, wrap the handler with `RetryHandler<H, P, W>` and provide a
`RetryPolicy` (default: exponential backoff, then dead-letter). Failed
events are written to a `*.dead_letter` topic via `DeadLetterWriter`,
preserving the original event plus failure metadata.

### Snapshots + Partitioning

- `Snapshot` / `SnapshotEventId` traits with a blanket impl over any
  `Serialize + DeserializeOwned + SnapshotEventId`. Use to persist
  aggregate state alongside the event log and resume rebuild from the
  snapshot's `EventId`.
- `EventKey::partition` (FNV-1a u64) for deterministic partition routing
  (used by Kafka writer for record key → partition selection, and by
  `PartitionedReader`).

### Reader Composition (Cursor + Checkpoint + Partitioned)

Backend readers (`PgReader`, `SqliteReader`, etc.) deliver events from a
single source. Cross-cutting concerns live in generic core wrappers in
`eventuary-core/src/io/readers/`:

- `PartitionedReader<R>` is an in-process lane scheduler. It routes inner
  messages into `LogicalPartition`s derived from `partition_for(event,
  count)`, buffers each lane up to `lane_capacity`, acks the inner source
  after accepting a message into a lane, and exposes one merged stream.
  Downstream `ack` clears that lane's in-flight slot; downstream `nack`
  requeues the same event at the front of that lane. Scheduling supports
  round-robin and queue-depth-weighted modes.
- `CheckpointReader<R, S>` composes a source reader with a
  `CheckpointStore`. On `read` it loads persisted cursors by
  `CheckpointScope`, starts the inner reader from the minimum stored
  cursor with `StartFrom::After(cursor)`, drops already-checkpointed
  messages per partition, and commits checkpoints only in **contiguous
  delivered order** per partition. `ack` calls the inner acker first and
  then commits synchronously so store errors propagate; `nack` leaves the
  checkpoint untouched.
- `CommitCursor` maps a delivery cursor to the value a checkpoint store
  persists. Source SQL cursors commit themselves; `PartitionedCursor<C>`
  strips the partition envelope and commits the inner source cursor while
  using its `LogicalPartition` in the `CheckpointKey`.
- `CheckpointStore<C>` is the persistence trait. SQL implementations
  (`PgCheckpointStore`, `SqliteCheckpointStore`) own the
  `consumer_offsets` table and persist today's integer source cursors
  (`PgCursor` / `SqliteCursor`).

Identity for the checkpoint store is `CheckpointKey { scope: { group,
stream_id }, partition: Option<LogicalPartition> }`. Unpartitioned
consumers serialize as `(partition = 0, partition_count = 1)`, matching
the default schema columns.

**Backend capability matrix.**

| Backend | delivery cursor | `CheckpointStore` | Notes |
|---|---|---|---|
| `eventuary-postgres` | `PgCursor` | ✅ `PgCheckpointStore<PgCursor>` | composes with `PartitionedReader` + `CheckpointReader` |
| `eventuary-sqlite` | `SqliteCursor` | ✅ `SqliteCheckpointStore<SqliteCursor>` | composes with `PartitionedReader` + `CheckpointReader` |
| `eventuary-memory` | `NoCursor` | — | mpsc source; no replay/checkpoint semantics |
| `eventuary-sqs` | `NoCursor` | — | queue visibility/delete is the native progress model |
| `eventuary-kafka` | `NoCursor` | — | consumer group commits are the native progress model |

Checkpoint compatibility is validated by the reader or wrapper that interprets
the cursor, not by a global topology fingerprint. `CheckpointReader` passes
checkpoint resume metadata to the inner subscription. `PartitionedReader`
validates stored logical partition metadata against its configured partition
count and returns `Error::InvalidCursor` when no compatible current checkpoint
rows exist. `CheckpointReader` then applies `CheckpointResumePolicy` to fail or
retry from the original subscription start.

### Error Model

A single `Error` enum in `eventuary::error`:

```
InvalidTopic / InvalidNamespace / InvalidOrganization /
InvalidMetadataKey / InvalidPayload / InvalidEventKey /
InvalidConsumerGroupId / InvalidStartFrom — validation
Serialization                                — encode/decode
Store                                        — backend-side
Timeout                                      — handler timeout, etc.
Config                                       — invalid backend config or
                                               contradictory subscription
```

`Result<T> = std::result::Result<T, Error>`. Backend crates produce
`Error::Store(...)` from their native error types via `map_err`. Keep
domain errors in the `Invalid*` family; do not promote backend failures
to domain errors.

## Conformance Suite

`eventuary-conformance` is being rewired alongside the cursor reader
refactor. The capability flag enum (`Capabilities`) still lives there;
the reusable case suite is being rebuilt to compose `PartitionedReader`
and `CheckpointReader` over each backend's source-cursor reader through
a per-backend subscription factory. Until that lands, per-backend
integration tests (`crates/eventuary-sqlite/tests`, etc.) cover the new
behavior directly.

## Backend Notes

### memory

- `mpsc::channel` per writer/reader pair. Test-only — no persistence.
- `NoopAcker`; subscription filter applied in `poll_next`.

### sqlite

- Bundled rusqlite (`features = ["bundled-full"]`). No external runtime.
- All DB ops wrapped in `tokio::task::spawn_blocking`.
- `SqliteReader` is a source reader over the configured events relation.
  It returns `Message<SqliteCursorAcker, SqliteCursor>` and only tracks
  cursor progress in memory for the active stream.
- `SqliteCheckpointStore<SqliteCursor>` owns the configured offsets
  relation and persists checkpoints by `(consumer_group_id, stream_id,
  partition, partition_count)` semantics.
- `SqliteDatabaseConfig`, `SqliteReaderConfig`, `SqliteWriterConfig`, and
  `SqliteCheckpointStoreConfig` all support validated relation names.
- Polling reader: `batch_size` + `poll_interval`.

### postgres

- `sqlx` with `runtime-tokio`. `pgvector` not required (that's an orchy concern).
- `PgReader` is a source reader over the configured events relation. It
  returns `Message<PgCursorAcker, PgCursor>` and only tracks cursor
  progress in memory for the active stream.
- `PgCheckpointStore<PgCursor>` owns the configured offsets relation and
  persists checkpoints by `(consumer_group_id, stream_id, partition,
  partition_count)` semantics.
- `PgDatabaseConfig`, `PgReaderConfig`, `PgWriterConfig`, and
  `PgCheckpointStoreConfig` all support validated simple or
  schema-qualified relation names.
- Integration tests use `testcontainers` with `postgres:18-alpine`.

### sqs

- `aws-sdk-sqs` long-polling (`wait_time_seconds`). Max 10 messages per
  receive (SQS limit, enforced in `validate()`).
- `SqsSubscription` carries queue URL, wait time, visibility timeout, max
  message count, `EventFilter`, and optional limit.
- `BatchedAcker<String>` token = receipt handle. `SqsFlusher` -> batch
  `DeleteMessageBatch` (10 per call).
- Queue semantics: no seek/replay cursor; delivered cursor is `NoCursor`.
- Localstack via `testcontainers`.

### kafka

- `rdkafka` with `cmake-build` + `tokio` features. Build requires
  `cmake`, `libssl-dev`, `libcurl4-openssl-dev`, `libsasl2-dev`,
  `zlib1g-dev`, `pkg-config`.
- `enable.auto.commit = false`; commits driven by `BatchedAcker` ->
  `KafkaFlusher::flush` (highest contiguous offset per partition, then
  `consumer.commit(CommitMode::Sync)`).
- `auto.offset.reset` and timestamp seek bound at construction. The
  consumer group is bound at construction too — a subscription that
  contradicts these is rejected with `Error::Config`.
- Confluent `cp-kafka` (KRaft mode) via `testcontainers`.

## Development

All commands run via `cargo` directly (no `just` recipe yet in this repo).
Iterate per sub-crate during development; rely on the umbrella for
consumer-facing verification.

```bash
cargo fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --lib       # unit tests, no containers
cargo test -p eventuary-memory     # memory tests
cargo test -p eventuary-sqlite     # sqlite tests, no containers

# Verify the umbrella for each feature combination consumers might use:
cargo check -p eventuary --no-default-features
cargo check -p eventuary --no-default-features --features "memory"
cargo check -p eventuary --no-default-features --features "postgres,kafka"
cargo test  -p eventuary --doc --all-features

# Container-based integration tests (rootless podman example):
export DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock
export TESTCONTAINERS_RYUK_DISABLED=true

cargo test -p eventuary-postgres -- --test-threads=1
cargo test -p eventuary-sqs      -- --test-threads=1
cargo test -p eventuary-kafka    -- --test-threads=1
```

CI (`.github/workflows/ci.yml`):

- `lint` — fmt + `clippy --workspace --all-targets --all-features`.
- `umbrella-feature-matrix` — `cargo check -p eventuary` against each
  feature combination consumers might pick (`""`, `memory`, `sqlite`,
  `postgres`, `sqs`, `kafka`).
- `test` — workspace unit tests + memory/sqlite integration + umbrella
  doctest with `--all-features`.
- `integration-postgres`, `integration-sqs`, `integration-kafka` — per-backend
  testcontainers runs.

When adding a new backend, update both the umbrella feature matrix and
add a per-backend integration job.

### Workspace Lints

Configured in the root `Cargo.toml` under `[workspace.lints]`:

- `unsafe_code = deny`
- `unreachable_pub = warn`
- `clippy::all = warn` (priority -1)
- explicitly elevated: `collapsible_if`, `clone_on_ref_ptr`,
  `needless_collect`, `redundant_closure`, `str_to_string`

Use `Arc::clone(&x)` instead of `x.clone()` for ref-counted pointers (the
`clone_on_ref_ptr` lint enforces this).

## Code Style

- **Rust edition 2024.** Use modern idioms: `let-else`, `if let && ...`
  chains, `LazyLock`, `slice::concat`, etc.
- **No comments** unless they are rustdoc (`///` or `//!`) targeted at
  docs.rs / library consumers. Inline `//` comments are a code smell —
  self-documenting names instead.
- **Trait first** in each file: trait definitions, then types,
  constructors, methods, getters. Helper functions at the bottom.
- **Return early / guard clauses.** No `else` after `return`.
- **No helper / utils files.** Cohesive module names that describe what
  they contain (`subscription.rs`, not `helpers.rs`).
- **Conventional commits.** `type(scope): description`. Common scopes:
  `core`, `memory`, `sqlite`, `postgres`, `sqs`, `kafka`, `conformance`,
  `ci`, `workflows`. Types: `feat`, `fix`, `refactor`, `test`, `docs`,
  `chore`, `perf`, `style`.
- **One coherent unit per commit.** Don't bundle unrelated fixes.
- **No `Co-Authored-By`.** No agent metadata in commits.
- **Never push.** Agents commit; the maintainer pushes.
- **Never stage** without explicit permission.
- **Do not change repo or global commit signing settings.** Agent-run
  signed commits often fail because GPG prompts. Use a one-off
  `git -c commit.gpgsign=false commit ...` only when the agent is
  executing the commit and interactive signing would block.

### Import Style

Import types, use short names everywhere except where module qualification
adds clarity.

- Types (structs, enums, traits): always import. Never write
  `std::sync::Arc`, `chrono::DateTime`, `sqlx::PgPool`, `uuid::Uuid`,
  or `serde_json::Value` inline.
- Stdlib: import and use short. `Arc::new()`, `HashMap::new()`,
  `Duration::from_secs()`.
- External crate functions: prefer short when readable. Keep
  `sqlx::query(...)` qualified; shorten `Utc::now()`, `Value::String(...)`.
- Internal `crate::` paths: import. Never write `crate::a::b::C` in a type
  position or expression.

## Decisions Log

Worth knowing when changing the codebase:

- **AFIT over `async-trait`.** Native `impl Future` return types for all
  IO traits; `async-trait` is intentionally absent. The dyn bridge
  (`DynWriter`, `DynReader`, …) provides type-erased variants that still
  implement the static trait.
- **Reader::Subscription + Reader::Cursor are associated types.** Each
  backend defines its own subscription struct and delivery cursor type;
  the dyn bridge is generic in both (`BoxReader<S, C>`).
- **Skip-ack on every filtered/poison path.** Otherwise the queue/stream
  offset never advances past unhandled records. SQL source readers
  advance their in-memory buffered-batch cursor on ack; Kafka/SQS
  skip-ack explicitly.
- **`AckBuffer + BatchFlusher`** pattern for Kafka/SQS, parameterized on a
  token type per backend. Background flusher task batches by count and
  interval, drives the protocol-specific commit.
- **FNV-1a partition hash on `EventKey`.** Deterministic and stable
  across machines and language ports; same key always routes to the
  same partition.
- **Reader composition over universal subscription.** Source readers do
  one job (deliver from the log with their native cursor). Lane fanout
  is `PartitionedReader<R>` in core; durable consumer progress is
  `CheckpointReader<R, S>` over a `CheckpointStore<C>` in core. SQL
  backends own only their `consumer_offsets` table via
  `PgCheckpointStore` / `SqliteCheckpointStore`; the checkpoint key is
  `{ scope: { group, stream_id }, partition: Option<LogicalPartition> }`.
  `CommitCursor` lets wrappers deliver enriched cursors while checkpoint
  stores persist source-native cursors.
- **Validated SQL relation names.** PostgreSQL and SQLite configs accept
  simple names (`events`) or schema-qualified names (`eventuary.events`)
  where supported. Always render through `PgRelationName` /
  `SqliteRelationName`; never interpolate unchecked relation strings.
- **`Snapshot` blanket impl** over `Serialize + DeserializeOwned +
  SnapshotEventId`. Aggregates only need to implement the marker
  `SnapshotEventId` to get JSON snapshotting for free.
- **`RetryHandler` + `DeadLetterWriter`** ship in core, not in a backend
  crate. Failed events become a new event on topic `<original>.dead_letter`,
  carrying the original payload + failure metadata.
- **UUID v7 everywhere** for time-ordered identifiers.
- **Core has zero backend feature flags.** `eventuary-core` deliberately
  knows nothing about backends. Feature flags live on the `eventuary`
  umbrella crate, where they map 1:1 to optional sub-crate dependencies.
  This keeps the core dep graph minimal for crates that only need the
  model (custom backends, in-process testing helpers, etc.).
- **Umbrella facade over flat single crate.** Eventuary uses the
  bevy/wgpu pattern: a thin top-level `eventuary` crate re-exports
  `eventuary-core` and feature-gated `eventuary-<backend>` crates.
  Trade-offs: more crate names visible on crates.io (one main + several
  sub-crates), but real multi-crate workspace for dev (per-crate test
  boundaries, smaller incremental builds, isolated dep graphs) plus a
  single import path for typical users.
- **Conformance suite is a sibling sub-crate, not under the umbrella.**
  `eventuary-conformance` depends only on `eventuary-core` and is
  intended as a `[dev-dependencies]` entry for backend authors. Keeping
  it out of the umbrella avoids dragging test-suite types into every
  consumer's dep graph.

## Releasing

Publishing is gated on a GitHub release / version tag. The
`.github/workflows/publish.yml` workflow runs only when:

1. A GitHub Release is published, or
2. A `vX.Y.Z` (or `vX.Y.Z-prerelease`) tag is pushed, or
3. A maintainer triggers `workflow_dispatch`.

The workflow verifies the tag matches `workspace.package.version`, runs
fmt + clippy + unit tests, then publishes in three tiers:

1. `eventuary-core` (waits 60s for crates.io index to settle).
2. `eventuary-memory`, `eventuary-sqlite`, `eventuary-postgres`,
   `eventuary-sqs`, `eventuary-kafka`, `eventuary-conformance` — all
   depend on `eventuary-core` only.
3. `eventuary` umbrella — depends on `eventuary-core` plus every backend
   crate via optional features.

Only the `eventuary-core` step gets a `cargo publish --dry-run` check;
the downstream crates can't dry-run from a clean machine because their
`eventuary-core = "..."` dep doesn't exist on crates.io yet. The real
publish steps run `cargo publish` which verifies the package each time.

A `CARGO_REGISTRY_TOKEN` repository secret must be configured. Migration
to [trusted publishing] is a future improvement.

### Release procedure

```bash
# 1. Bump workspace.package.version in the root Cargo.toml
# 2. Commit + push (maintainer)
# 3. Tag + push tag
git tag v0.1.0-alpha.1
git push origin v0.1.0-alpha.1
# 4. Publish workflow runs automatically.
```

[trusted publishing]: https://crates.io/docs/trusted-publishing

## License

MIT.
