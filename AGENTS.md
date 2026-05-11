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
   `Message<A>`) using native AFIT plus a dyn bridge (`BoxWriter`,
   `BoxReader`, `BoxHandler`, …) for runtime composition. Also in
   `eventuary-core`.
3. **Backend crates** that implement those traits over real systems —
   `eventuary-memory`, `eventuary-sqlite`, `eventuary-postgres`,
   `eventuary-sqs`, `eventuary-kafka` — plus an `eventuary-conformance`
   crate with reusable test cases.

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
│       ├── event_key.rs    # EventKey + PartitionKey impl (FNV-1a)
│       ├── topic.rs        # Topic (dot-separated, lowercase/digits/_/-)
│       ├── namespace.rs    # Namespace (slash-rooted hierarchy)
│       ├── organization.rs # OrganizationId (tenant; "_platform" sentinel)
│       ├── consumer_group_id.rs # ConsumerGroupId (1..=64 chars)
│       ├── payload.rs      # Payload + ContentType (JSON / text / binary)
│       ├── metadata.rs     # Metadata key/value pairs
│       ├── collector.rs    # EventCollector (aggregate -> drain -> persist)
│       ├── partition.rs    # PartitionKey trait
│       ├── snapshot.rs     # Snapshot + SnapshotEventId
│       ├── start_from.rs   # StartFrom (Earliest / Latest / Timestamp)
│       ├── serialization.rs # SerializedEvent wire format
│       ├── error.rs        # Error enum, Result alias
│       └── io/
│           ├── writer.rs   # Writer trait + DynWriter / BoxWriter / ArcWriter
│           ├── reader.rs   # Reader trait (assoc Subscription/Acker/Stream)
│           ├── handler.rs  # Handler + Filter + dyn bridges
│           ├── message.rs  # Message<A> (event + acker)
│           ├── filters.rs  # AllFilter, TopicFilter, NamespacePrefixFilter
│           ├── subscription.rs # EventSubscription (read-time scope + Filter impl)
│           ├── acker/
│           │   ├── noop.rs      # NoopAcker
│           │   ├── once.rs      # OnceAcker (single-shot wrapper)
│           │   ├── batched.rs   # BatchedAcker + AckBuffer + BatchFlusher
│           │   └── either.rs    # Re-exports Either for variant ackers
│           └── consumers/
│               ├── background.rs # BackgroundConsumer + ConsumerHandle
│               └── retry.rs      # RetryHandler, RetryPolicy, DeadLetterWriter
│
├── eventuary-memory/       # in-memory tokio::mpsc backend; NoopAcker
├── eventuary-sqlite/       # rusqlite (bundled), spawn_blocking, consumer_offsets table
├── eventuary-postgres/     # sqlx Postgres, consumer_offsets table
├── eventuary-sqs/          # aws-sdk-sqs, BatchedAcker via SqsFlusher
├── eventuary-kafka/        # rdkafka StreamConsumer, BatchedAcker via KafkaFlusher
└── eventuary-conformance/  # shared Backend trait + test cases (roundtrip,
                            #   ack/nack semantics, consumer groups, filters,
                            #   start_from variants, ordering)
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

The conformance suite is exposed only as a **direct sub-crate**
(`eventuary-conformance`), not re-exported through the umbrella. Backend
authors add it as a `[dev-dependencies]` entry, alongside `eventuary-core`.

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
reconstructed without revalidation via `Event::restore(RestoreEvent { ... })`.
UUID v7 is used for `EventId` so events are time-ordered.

### Constructor Convention

| Pattern | Purpose | Validates? |
|---------|---------|------------|
| `T::new(...)` | First-time construction (value objects, aggregates) | Yes |
| `Event::create(...)` | Domain creation that may emit events | Yes |
| `Event::restore(RestoreEvent { ... })` | Reconstruction from persisted state | No |

`Restore*` structs use named public fields, not positional params. Never
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
    type Stream: Stream<Item = Result<Message<Self::Acker>>> + Send;
    fn read(&self, subscription: Self::Subscription) -> impl Future<Output = Result<Self::Stream>> + Send;
}
```

For runtime composition / DI, every trait has a `Dyn*` sibling and
`Box*` / `Arc*` aliases:

| Static trait | Dyn trait | Boxed alias | Arc alias |
|--------------|-----------|-------------|-----------|
| `Writer` | `DynWriter` | `BoxWriter` | `ArcWriter` |
| `Reader` | `DynReader<S, A>` | `BoxReader<S, A>` | `ArcReader<S, A>` |
| `Handler` | `DynHandler` | `BoxHandler` | `ArcHandler` |
| `Acker` | `DynAcker` | `BoxAcker` | `ArcAcker` |
| `Filter` | — | `BoxFilter` | `ArcFilter` |

Use `WriterExt::into_boxed()` / `into_arced()`, `ReaderExt::into_boxed()` /
`into_arced()`, etc. to erase. `Box<dyn DynWriter>` still implements
`Writer`, so erased values can pass back through generic APIs.

`Reader::Subscription` is an associated type so each backend can specialize
(Kafka has a richer config than memory). The dyn bridge defaults to
`EventSubscription` for the common case.

### Subscription Model

`EventSubscription` (see `crates/eventuary/src/subscription.rs`) is the
read-side request:

- **Identity:** `name` (optional label), `consumer_group_id`
  (offset-bookkeeping identity for backends that support it).
- **Tenant scope:** `organization` (always required).
- **Filter predicates:** `topics`, `namespace_prefix`, `keys`, `metadata`,
  `end_at`. AND across fields; the metadata field is a subset match
  (AND across pairs, no OR).
- **Positional / count:** `start_from`, `limit`.

`subscription.matches(event)` is the in-memory predicate used by every
backend after deserialization. SQL backends (sqlite, postgres) push the
topic, namespace, organization, and start-from filters into the SQL query
to prune at the DB; Kafka/SQS apply them in the consumer loop.

Backends may **reject** subscriptions that contradict construction-time
bindings — Kafka binds the consumer group and offset behavior at
construction; SQS only honors `StartFrom::Latest`. Document such
constraints on the backend's `Reader::read` impl.

### Message + Acker

`Message<A: Acker>` pairs an `Event` with an `Acker`. The handler receives
the **owned event** (`Handler::handle(&self, event: Event)`); the ack/nack
envelope stays with the consumer driver, not the handler.

Ack semantics are backend-specific:

| Backend | ack | nack |
|---------|-----|------|
| memory | no-op (`NoopAcker`) | no-op |
| sqlite/postgres | upsert into `consumer_offsets`, guarded by `excluded.sequence > consumer_offsets.sequence` | no-op (offset unchanged) |
| sqs | `BatchedAcker` -> `SqsFlusher` -> `DeleteMessageBatch` | no-op; visibility timeout redelivers |
| kafka | `BatchedAcker` -> `KafkaFlusher` -> `consumer.commit` with highest contiguous offset per partition | no-op; offset left uncommitted, redelivered after rebalance/restart |

**Skip-ack rule:** when a backend filters out an event (org mismatch,
subscription mismatch, or poison record that fails to deserialize), it
**must ack** so the underlying offset advances. Otherwise readers stall
behind a record they will never deliver. Apply this in every skip path,
not just the obvious ones.

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
- `PartitionKey` trait with FNV-1a implementation on `EventKey` for
  deterministic partition routing (used by Kafka writer for record key
  → partition selection).

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

`eventuary-conformance` defines a `Backend` trait and ~12 reusable test
cases:

- `case_write_read_roundtrip`
- `case_write_all_preserves_all_events`
- `case_ordering_preserved`
- `case_topic_filter`
- `case_namespace_prefix_filter`
- `case_start_from_earliest` / `_latest` / `_timestamp`
- `case_ack_advances_checkpoint`
- `case_nack_does_not_advance_checkpoint`
- `case_independent_consumer_groups`
- `case_independent_streams_within_group`

Each backend test crate provides a `Backend` impl + `Capabilities` flag
set, then calls `run_all(&backend)`. Capabilities (`supports_replay`,
`supports_timestamp_start`, `supports_nack_redelivery`,
`preserves_total_order`, `supports_consumer_groups`,
`supports_independent_streams`) gate which cases run for that backend.

When adding a backend, **first** plug it into the conformance suite,
**then** add backend-specific tests for native features.

## Backend Notes

### memory

- `mpsc::channel` per writer/reader pair. Test-only — no persistence.
- `NoopAcker`; subscription filter applied in `poll_next`.

### sqlite

- Bundled rusqlite (`features = ["bundled-full"]`). No external runtime.
- All DB ops wrapped in `tokio::task::spawn_blocking`.
- `consumer_offsets(organization, consumer_group_id, stream, sequence)` table.
- `Either<NoopAcker, OnceAcker<SqliteAcker>>` — `OnceAcker` when a group
  is set, `NoopAcker` otherwise.
- Polling reader: `batch_size` + `poll_interval`.

### postgres

- `sqlx` with `runtime-tokio`. `pgvector` not required (that's an orchy concern).
- Same `consumer_offsets` schema and Either-acker shape as sqlite.
- Integration tests use `testcontainers` with `postgres:16`.

### sqs

- `aws-sdk-sqs` long-polling (`wait_time_seconds`). Max 10 messages per
  receive (SQS limit, enforced in `validate()`).
- `BatchedAcker<String>` token = receipt handle. `SqsFlusher` -> batch
  `DeleteMessageBatch` (10 per call).
- Only `StartFrom::Latest` is honored (queue semantics — no seek).
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
- **Reader::Subscription is an associated type.** Each backend can specialize
  what arguments it accepts; the dyn bridge defaults to `EventSubscription`
  for the common case.
- **`EventSubscription` replaced `EventSelector`.** Single struct holding
  filter predicates + identity, with `matches(&Event) -> bool`. Backends
  use it both as a SQL/protocol hint and as the in-memory predicate.
- **Skip-ack on every filtered/poison path.** Otherwise the offset never
  advances past unhandled records. SQL backends advance the cursor
  by sequence; Kafka/SQS skip-ack explicitly.
- **`Either<NoopAcker, OnceAcker<...>>`** for sqlite/postgres so callers
  without a `consumer_group_id` get a cheap noop and callers with one get
  single-shot semantics that move the persisted offset forward exactly
  once per event.
- **`AckBuffer + BatchFlusher`** pattern for Kafka/SQS, parameterized on a
  token type per backend. Background flusher task batches by count and
  interval, drives the protocol-specific commit.
- **`PartitionKey` trait + FNV-1a on `EventKey`.** Deterministic and
  stable across machines and language ports; same key always routes to
  the same partition.
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
