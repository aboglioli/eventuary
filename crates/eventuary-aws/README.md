# eventuary-aws

AWS event backends for [eventuary](https://crates.io/crates/eventuary). Supersedes `eventuary-sqs`.

This crate hosts every AWS-backed implementation of the eventuary IO traits. Because it covers more than one AWS service, its modules are nested by **service first, role second** — `eventuary_aws::sqs::reader::SqsReader`, `eventuary_aws::sns::writer::SnsWriter` — rather than exposing role modules at the crate root like the single-service backend crates do.

| Path | Role |
|------|------|
| `sqs::writer::SqsWriter` | publishes serialized events via `SendMessage` / `SendMessageBatch` |
| `sqs::reader::SqsReader` | long-polls a queue, emits `Message<BatchedAcker<String>, NoCursor>` keyed by receipt handle |
| `sqs::flusher::SqsFlusher` | batched `DeleteMessageBatch` (ack) and `ChangeMessageVisibilityBatch` (nack) |
| `sns::writer::SnsWriter` | topic fanout via `Publish` / `PublishBatch` |

## SQS

`SqsWriter` serializes events with `SerializedEvent::to_json_string`. `SqsReader` long-polls `ReceiveMessage` and emits messages whose ack token is the SQS receipt handle. Acks are batched into `DeleteMessageBatch` calls; nacks become `ChangeMessageVisibilityBatch` with a zero visibility timeout, so the message is redelivered immediately. SQS only supports `StartFrom::Latest`; `Earliest`, `Timestamp`, `limit`, and `consumer_group_id` are rejected at config construction with `Error::Config`. Poison records (missing body, malformed JSON, undecodable event) are acked and skipped so the queue keeps draining.

## SNS

`SnsWriter` publishes to a topic using the same wire format as every other durable backend, so a subscribed queue can be consumed with `SqsReader`.

**SNS is publish-only — there is no receive API, so this crate ships no `SnsReader`.** The canonical topology is SNS → SQS fanout: publish once, subscribe one queue per consumer, read each with `SqsReader`.

```text
                       ┌── queue A ── SqsReader ── projection
SnsWriter ── topic ────┤
                       └── queue B ── SqsReader ── audit log
```

### Raw message delivery is required

Subscriptions must have `RawMessageDelivery` enabled:

```bash
aws sns set-subscription-attributes \
  --subscription-arn <arn> \
  --attribute-name RawMessageDelivery \
  --attribute-value true
```

Without it SNS wraps the body in a notification envelope (`{"Type":"Notification","Message":"<body>",...}`) that `SqsReader` cannot decode as a `SerializedEvent`. Every delivered event would be treated as a poison record and silently ack-skipped — the queue drains and nothing reaches your handler.

### Topic types

`SnsTopicType` selects the topic the writer is addressing and maps SNS FIFO requirements onto the event's own identities:

| `SnsTopicType` | `MessageGroupId` | `MessageDeduplicationId` |
|------|------------------|--------------------------|
| `Standard` (default) | — | — |
| `Fifo` | `event.key()` | `event.id()` |
| `FifoContentBasedDeduplication` | `event.key()` | derived by SNS from the body |

`event.key()` is the required routing identity, so all events for one entity land in the same message group and stay ordered relative to each other. `event.id()` is a unique UUID v7, which is exactly what a deduplication id needs to be.

```rust,ignore
use eventuary_aws::sns::writer::{SnsTopicType, SnsWriter, SnsWriterConfig};

let writer = SnsWriter::new_with_config(
    sns_client,
    "arn:aws:sns:us-east-1:123456789012:orders.fifo",
    SnsWriterConfig { topic_type: SnsTopicType::Fifo },
);
```

## Testing

Integration tests use [`testcontainers`](https://crates.io/crates/testcontainers) to spawn LocalStack at `localstack/localstack:3.8.1`, covering the SQS writer/reader/flusher and the SNS writer including end-to-end SNS → SQS delivery. On rootless podman:

```bash
export DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock
export TESTCONTAINERS_RYUK_DISABLED=true

cargo test -p eventuary-aws -- --test-threads=1
```
