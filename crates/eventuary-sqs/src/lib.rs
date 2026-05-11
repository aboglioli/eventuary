//! AWS SQS event backend for [eventuary](https://crates.io/crates/eventuary).
//!
//! `SqsWriter` serializes events with `SerializedEvent::to_json_string` and
//! sends them via `SendMessageBatch`. `SqsReader` long-polls `ReceiveMessage`
//! and emits `Message<BatchedAcker<String>>` whose token is the receipt handle.
//!
//! SQS does not support historical replay: `StartFrom::{Earliest, Timestamp}`,
//! `end_at`, and `limit` are rejected at config time with `Error::Config`.
//! Records whose embedded organization does not match the configured one are
//! acked and skipped.
//!
//! ack issues `DeleteMessageBatch`; nack issues `ChangeMessageVisibilityBatch`
//! with timeout zero. Both inspect per-entry failures.

mod flusher;
mod reader;
mod reader_config;
mod writer;

pub use flusher::SqsFlusher;
pub use reader::{SqsReader, SqsStream};
pub use reader_config::SqsReaderConfig;
pub use writer::SqsWriter;
