//! Apache Kafka event backend for [eventuary](https://crates.io/crates/eventuary).
//!
//! Wraps `rdkafka`. `KafkaWriter` produces records (event key as record key,
//! JSON-encoded `SerializedEvent` as payload). `KafkaReader` streams from a
//! `StreamConsumer` and emits `Message<BatchedAcker<KafkaOffsetToken>>` whose
//! token carries `(topic, partition, offset)`.
//!
//! Configuration separates `kafka_topics: Vec<String>` (Kafka topic names to
//! subscribe) from `event_topics: Option<Vec<Topic>>` (post-deserialization
//! filter on the eventuary topic). `organization` and `namespace` are optional
//! post-deserialization filters. `StartFrom::{Earliest, Latest, Timestamp}` are honored.
//!
//! ack commits the highest offset per partition; nack is a no-op (uncommitted
//! offsets are redelivered after rebalance/restart/session expiry).
//!
//! Building requires `cmake`, `libssl-dev`, `libcurl4-openssl-dev`,
//! `libsasl2-dev`, `zlib1g-dev`, and `pkg-config`.

mod flusher;
mod reader;
mod reader_config;
mod writer;

pub use flusher::{KafkaFlusher, KafkaOffsetToken};
pub use reader::{KafkaReader, KafkaStream};
pub use reader_config::KafkaReaderConfig;
pub use writer::KafkaWriter;
