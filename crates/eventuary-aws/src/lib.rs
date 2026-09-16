//! AWS event backends for [eventuary](https://crates.io/crates/eventuary).
//!
//! Modules nest by service first and role second: `sqs::reader::SqsReader`,
//! `sns::writer::SnsWriter`. This crate supersedes `eventuary-sqs`.
//!
//! `SqsWriter` serializes events with `SerializedEvent::to_json_string` and
//! sends them via `SendMessageBatch`. `SqsReader` long-polls `ReceiveMessage`
//! and emits `Message<BatchedAcker<String>, NoCursor>` whose token is the receipt handle.
//! SQS does not support historical replay: `StartFrom::{Earliest, Timestamp}`
//! and `limit` are rejected at config time with `Error::Config`. Poison
//! records (missing body, undecodable event) are acked and skipped.
//!
//! `SnsWriter` publishes the same wire format via `Publish` / `PublishBatch`.
//! SNS is publish-only and has no receive API, so there is no `SnsReader`:
//! consume a topic by subscribing SQS queues to it and reading each with
//! `SqsReader`. Those subscriptions must set `RawMessageDelivery=true`,
//! otherwise SNS wraps the body in a notification envelope that `SqsReader`
//! decodes as a poison record and ack-skips.
//!
//! `SnsFifoConfig` derives FIFO attributes from event identity: `MessageGroupId`
//! from `Event::key()` and `MessageDeduplicationId` from `Event::id()`.

pub mod sns;
pub mod sqs;
