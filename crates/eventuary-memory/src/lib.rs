//! In-memory event backend for [eventuary](https://crates.io/crates/eventuary).
//!
//! Provides [`writer::MemoryWriter`] and [`reader::MemoryReader`] backed by `tokio::sync::mpsc`
//! channels. Events emit `Message<NoopAcker>`, so ack/nack are no-ops. Suitable
//! for development, tests, and single-process use.

pub mod reader;
pub mod writer;
