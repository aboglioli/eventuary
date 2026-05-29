//! In-memory event backend for [eventuary](https://crates.io/crates/eventuary).
//!
//! Provides [`writer::MemoryWriter`] and [`reader::MemoryReader`] backed by `tokio::sync::mpsc`
//! channels. Events emit `Message<NoopAcker, NoCursor>`, so ack/nack are no-ops. Suitable
//! for development, tests, and single-process use.
//!
//! All store implementations are exposed through their role modules.

pub mod buffer;
pub mod checkpoint;
pub mod claim_buffer;
pub mod coordinator;
pub mod dedupe;
pub mod multiplexer;
pub mod reader;
pub mod subscriber_work;
pub mod watermark;
pub mod writer;
