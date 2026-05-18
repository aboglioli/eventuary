//! In-memory event backend for [eventuary](https://crates.io/crates/eventuary).
//!
//! Provides [`writer::MemoryWriter`] and [`reader::MemoryReader`] backed by `tokio::sync::mpsc`
//! channels. Events emit `Message<NoopAcker>`, so ack/nack are no-ops. Suitable
//! for development, tests, and single-process use.
//!
//! Also ships in-memory implementations of the IO store traits:
//! - [`MemoryMultiplexerStore`]
//! - [`MemoryBufferStore`]
//! - [`MemoryDedupeStore`]

pub mod buffer_store;
pub mod dedupe_store;
pub mod multiplexer_store;
pub mod reader;
pub mod watermark_store;
pub mod writer;

pub use buffer_store::{MemoryBufferStore, MemoryBufferStoreId};
pub use dedupe_store::MemoryDedupeStore;
pub use multiplexer_store::MemoryMultiplexerStore;
pub use reader::MemoryReader;
pub use watermark_store::MemoryWatermarkStore;
pub use writer::MemoryWriter;
