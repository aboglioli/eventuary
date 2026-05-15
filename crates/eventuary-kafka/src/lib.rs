//! Apache Kafka event backend for [eventuary](https://crates.io/crates/eventuary).
//!
//! Wraps `rdkafka`. `KafkaWriter` produces records (event key as record key,
//! JSON-encoded `SerializedEvent` as payload). `KafkaReader` streams from a
//! `StreamConsumer` and emits `Message<BatchedAcker<KafkaOffsetToken>>` whose
//! token carries `(topic, partition, offset)`.
//!
//! Configuration uses `kafka_topics: Vec<String>` (Kafka topic names to
//! subscribe). Event-level filtering is not performed by this reader; wrap with
//! `FilteredReader` if predicate-based filtering on deserialized events is
//! needed. `StartFrom::{Earliest, Latest, Timestamp}` are honored.
//!
//! ack commits the highest offset per partition; nack is a no-op (uncommitted
//! offsets are redelivered after rebalance/restart/session expiry).
//!
//! Building requires `cmake`, `libssl-dev`, `libcurl4-openssl-dev`,
//! `libsasl2-dev`, `zlib1g-dev`, and `pkg-config`.

pub mod flusher;
pub mod reader;
pub mod reader_config;
pub mod writer;

pub use reader::KafkaReader;
pub use writer::KafkaWriter;
