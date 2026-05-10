//! In-memory event backend for [eventuary](https://crates.io/crates/eventuary).
//!
//! Provides [`InmemWriter`] and [`InmemReader`] backed by `tokio::sync::mpsc`
//! channels. Events emit `Message<NoopAcker>`, so ack/nack are no-ops. Suitable
//! for development, tests, and single-process use.

mod reader;
mod writer;

pub use reader::InmemReader;
pub use writer::InmemWriter;
