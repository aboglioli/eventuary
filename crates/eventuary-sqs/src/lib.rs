//! AWS SQS event backend for [eventuary](https://crates.io/crates/eventuary).
//!
//! `SqsWriter` serializes events with `SerializedEvent::to_json_string` and
//! sends them via `SendMessageBatch`. `SqsReader` long-polls `ReceiveMessage`
//! and emits `Message<BatchedAcker<String>>` whose token is the receipt handle.
//!
//! SQS does not support historical replay: `StartFrom::{Earliest, Timestamp}`
//! and `limit` are rejected at config time with `Error::Config`. Poison
//! records (missing body, undecodable event) are acked and skipped.
//!
//! ack issues `DeleteMessageBatch`; nack issues `ChangeMessageVisibilityBatch`
//! with timeout zero. Both inspect per-entry failures.

mod flusher;
mod reader;
mod reader_config;
mod writer;

pub use flusher::SqsFlusher;
pub use reader::{SqsReader, SqsStream, SqsSubscription};
pub use reader_config::SqsReaderConfig;
pub use writer::SqsWriter;
