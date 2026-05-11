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
use eventuary::{Event, Handler};

struct LogHandler;

impl Handler for LogHandler {
    fn id(&self) -> &str { "log-handler" }

    fn handle(&self, event: Event)
        -> impl std::future::Future<Output = eventuary::Result<()>> + Send
    {
        async move {
            tracing::info!(topic = %event.topic(), "received");
            Ok(())
        }
    }
}
```

`Handler::handle` receives an owned `Event` — the ack/nack envelope is owned
by the `Reader`'s `Message<A>` and managed by the consumer driver, not by the
handler.

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
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --lib       # unit tests, no containers
cargo test -p eventuary-memory     # memory tests
cargo test -p eventuary-sqlite     # sqlite tests, no containers
```

## Releasing

Publishing to crates.io is gated on a release event — pushes to `main` never
publish. The workflow at `.github/workflows/publish.yml` runs only when:

1. A GitHub Release is published (`release: published`), or
2. A `vX.Y.Z` (or `vX.Y.Z-prerelease`) tag is pushed, or
3. A maintainer triggers `workflow_dispatch` (with optional dry-run input).

The workflow verifies the tag matches `workspace.package.version`, runs fmt /
clippy / unit tests, executes `cargo publish --dry-run` for every crate, then
publishes the six crates in dependency order with a 60-second pause after the
core crate to let the index settle.

### Release procedure

```bash
# 1. Bump version in root Cargo.toml under [workspace.package]
#    (all six crates inherit via workspace.package.version)

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
