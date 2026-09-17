//! AWS event backends for [eventuary](https://crates.io/crates/eventuary).
//!
//! Modules nest by service first and role second: `sqs::reader::SqsReader`,
//! `sns::writer::SnsWriter`. This crate supersedes `eventuary-sqs`.
//!
//! `SqsWriter` serializes events with `SerializedEvent::to_json_string` and
//! sends them via `SendMessageBatch`. `SqsReader` long-polls `ReceiveMessage`
//! and emits `Message<BatchedAcker<String>, NoCursor>` whose token is the receipt handle.
//! SQS has no replay cursor, so `SqsReaderConfig` carries no start position at
//! all rather than accepting one it would reject. `SqsSubscription` owns the
//! protocol bounds and is validated on every read, not only when the reader is
//! constructed. A message the
//! reader cannot decode into an `Event` is logged at warn with its message id
//! and the decode error, then deleted, because leaving it on the queue would
//! redeliver it forever.
//!
//! `SnsWriter` publishes the same wire format via `Publish` / `PublishBatch`.
//! SNS is publish-only and has no receive API, so there is no `SnsReader`:
//! consume a topic by subscribing SQS queues to it and reading each with
//! `SqsReader`. Those subscriptions must set `RawMessageDelivery=true`,
//! otherwise SNS wraps the body in a notification envelope that `SqsReader`
//! decodes as a poison record and ack-skips.
//!
//! `SqsQueueType` and `SnsTopicType` select a standard or FIFO queue/topic and
//! derive FIFO attributes from event identity: `MessageGroupId` from
//! `Event::key()` and `MessageDeduplicationId` from `Event::id()`. Pass them
//! through `SqsWriterConfig` / `SnsWriterConfig`; `SqsReaderConfig` takes the
//! queue type too, so FIFO polls carry a `ReceiveRequestAttemptId` and a
//! retried receive returns the same messages instead of stalling the group.

mod batch;
mod request;

pub mod sns;
pub mod sqs;
