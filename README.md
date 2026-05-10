# Eventuary

Eventuary is a Rust event toolkit for logs, queues, streams, routing, replay,
and acknowledgements across multiple backends.

It provides a small core event model and async IO traits, plus backend
implementations for memory, SQLite, Postgres, SQS, and Kafka.

> **Status:** Alpha (`0.1.0-alpha.0`). API may change before `0.1.0`.

## Crates

| Crate | Description |
|-------|-------------|
| [`eventuary`](crates/eventuary) | Core event model and async IO traits |
| [`eventuary-memory`](crates/eventuary-memory) | In-memory backend (dev/test) |
| [`eventuary-sqlite`](crates/eventuary-sqlite) | SQLite event log backend |
| [`eventuary-postgres`](crates/eventuary-postgres) | PostgreSQL event log backend |
| [`eventuary-sqs`](crates/eventuary-sqs) | AWS SQS backend |
| [`eventuary-kafka`](crates/eventuary-kafka) | Apache Kafka backend |

Backend crates are independently published. Pick the ones you need:

```toml
[dependencies]
eventuary = "0.1.0-alpha.0"
eventuary-postgres = "0.1.0-alpha.0"
```

The core `eventuary` crate has no backend feature flags — backends live in
their own crates. This avoids dependency cycles and lets each backend release
on its own cadence.

## Core Model

```rust
use eventuary::{Event, Payload};

let event = Event::create(
    "acme",
    "/billing",
    "invoice.created",
    "invoice-123",
    Payload::from_json(&serde_json::json!({ "amount": 100 }))?,
)?;
```

- `Event` — immutable record with `id` (UUID v7), `organization`, `namespace`,
  `topic`, `key`, `payload`, `metadata`, `timestamp`, `version`.
- `Payload` — JSON, plain text, or binary content.
- `Topic` — dot-separated, lowercase/digit/`_`/`-`.
- `Namespace` — slash-rooted hierarchy: `/`, `/billing`, `/billing/invoices`.
- `OrganizationId` — tenant scope; `_platform` sentinel for cross-tenant.
- `Metadata` — validated key/value pairs.
- `SerializedEvent` — wire format with `to_json_value` / `from_json_value` /
  `to_json_string` / `from_json_str` helpers used by every backend.

## Async IO

```rust
use eventuary::{Event, Payload, Writer};

async fn emit<W: Writer>(writer: &W, event: &Event) -> eventuary::Result<()> {
    writer.write(event).await
}
```

Native async traits with `impl Future` return types. No `async-trait`. A dyn
bridge (`DynWriter`, `BoxWriter`, `ArcWriter` and same for `Reader`,
`Handler`, `Acker`) is provided for runtime composition and DI.

```rust
use eventuary::{BoxWriter, Event, Writer};

async fn emit_boxed(writer: &BoxWriter, event: &Event) -> eventuary::Result<()> {
    writer.write(event).await
}
```

## In-Memory Backend Example

```rust
use eventuary::{Event, Payload, Reader, Writer};
use eventuary_memory::{InmemReader, InmemWriter};
use tokio::sync::mpsc;

let (tx, rx) = mpsc::channel(100);
let writer = InmemWriter::new(tx);
let mut reader = InmemReader::new(rx);

let event = Event::create(
    "acme",
    "/orders",
    "order.placed",
    "order-1",
    Payload::from_json(&serde_json::json!({ "total": 42 }))?,
)?;

writer.write(&event).await?;
```

## Consumer Loop

```rust
use eventuary::{BackgroundConsumer, Handler};
use std::sync::Arc;

struct LogHandler;

impl Handler for LogHandler {
    fn handle<'a>(&'a self, msg: eventuary::Message<eventuary::io::acker::NoopAcker>)
        -> impl std::future::Future<Output = eventuary::Result<()>> + Send + 'a
    {
        async move {
            tracing::info!(topic = %msg.event().topic(), "received");
            Ok(())
        }
    }
}
```

`BackgroundConsumer` polls a `Reader`, applies an optional filter, runs a
`Handler` per event, and acks/nacks based on the handler result. Timeouts nack.

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

The Kafka backend requires `cmake`, `libssl-dev`, and `pkg-config` to build
`librdkafka` (via the `cmake-build` rdkafka feature).

## Development

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --lib       # unit tests, no containers
cargo test -p eventuary-memory     # memory tests
cargo test -p eventuary-sqlite     # sqlite tests, no containers
```

## Publishing

A `release: published` GitHub Actions workflow at
`.github/workflows/publish.yml` publishes all six crates in order. It is not
triggered automatically — a maintainer creates a release manually after
verifying all backends still pass on the release branch.

## License

MIT — see [LICENSE](LICENSE).
