//! Filesystem event log backend for [eventuary](https://crates.io/crates/eventuary).
//!
//! An append-only log on ordinary files, organised the way a log-structured
//! broker organises one: the log is split into partitions, each partition is a
//! directory of size-rolled segments, and every segment carries sparse offset
//! and time indexes so a read can seek near its target instead of scanning
//! from the start.
//!
//! ```text
//! <root>/
//! ├── meta.json                              format version, partition count
//! ├── 00000/                                 partition 0
//! │   ├── .lock                              exclusive writer lock
//! │   ├── 00000000000000000000.log           segment, named by base offset
//! │   ├── 00000000000000000000.index         sparse offset -> byte position
//! │   ├── 00000000000000000000.timeindex     sparse timestamp -> offset
//! │   └── 00000000000000004096.log           next segment after a roll
//! ├── 00001/
//! └── checkpoints/<group>/<stream>/<cursor>.json
//! ```
//!
//! Each segment line is one JSON object: the serialized event with a flat
//! `offset` field, so the log stays readable with `cat`, `grep` and `jq`.
//!
//! # Guarantees
//!
//! - Offsets are dense, monotonic and gapless within a partition, assigned by
//!   the writer that holds the partition lock.
//! - A partition has at most one writer at a time, enforced by an advisory
//!   file lock released automatically when the process exits.
//! - A torn trailing write is truncated on open; complete records before it are
//!   preserved, and the next append reuses the offset that was lost.
//! - Readers take no locks and observe appends as they land.
//!
//! # Components
//!
//! - [`writer::FsWriter`] implements [`eventuary_core::io::Writer`], routing
//!   events to partitions with the same resolver and hasher as every other
//!   partition-aware backend.
//! - [`reader::FsReader`] implements [`eventuary_core::io::Reader`], with
//!   [`reader::FsSubscription`] supporting start positions, stop positions,
//!   filters, partition selection and per-partition resume.
//! - [`checkpoint::FsCheckpointStore`] implements
//!   [`eventuary_core::io::reader::CheckpointStore`] over one JSON file per
//!   cursor, written atomically.
//! - [`coordinator::FsPartitionCoordinator`] implements
//!   [`eventuary_core::io::reader::PartitionCoordinator`], so several processes
//!   can share a consumer group: partitions are claimed under fenced,
//!   generation-checked leases with the same semantics as the SQL backends, and
//!   [`reader::FsCoordinatedReader`] composes the two.
//! - [`buffer::FsBufferStore`], [`dedupe::FsDedupeStore`],
//!   [`multiplexer::FsMultiplexerStore`] and [`watermark::FsWatermarkStore`]
//!   implement the remaining reader and handler store traits, so the composers
//!   in [`eventuary_core::io`] work against files exactly as they do against a
//!   database.
//! - [`log::PartitionLog`] is the storage engine underneath, usable directly
//!   when the trait surface is more than a caller needs.
//! - [`layout`] documents the on-disk path conventions.

mod atomic;
pub mod buffer;
pub mod checkpoint;
pub mod coordinator;
pub mod dedupe;
mod error;
mod index;
pub mod layout;
pub mod log;
mod meta;
pub mod multiplexer;
pub mod partitioning;
pub mod reader;
mod record;
mod segment;
pub mod watermark;
pub mod writer;
