//! AWS event backends for [eventuary](https://crates.io/crates/eventuary).
//!
//! Modules nest by service first and role second: `sqs::reader::SqsReader`.
//! This crate supersedes `eventuary-sqs`.
//!
//! `SqsWriter` serializes events with `SerializedEvent::to_json_string` and
//! sends them via `SendMessageBatch`. `SqsReader` long-polls `ReceiveMessage`
//! and emits `Message<BatchedAcker<String>, NoCursor>` whose token is the receipt handle.
//! SQS does not support historical replay: `StartFrom::{Earliest, Timestamp}`
//! and `limit` are rejected at config time with `Error::Config`. Poison
//! records (missing body, undecodable event) are acked and skipped.

pub mod sqs;
