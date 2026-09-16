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
//! - A torn trailing write is truncated when the writer opens the partition;
//!   complete records before it are preserved, and the next append reuses the
//!   offset that was lost. Readers never repair, so tailing a partition cannot
//!   rewrite it under its owner.
//! - Readers take no locks and observe appends as they land.
//! - [`checkpoint`], [`coordinator`], [`buffer`] and [`watermark`] state is
//!   written to a temporary file, fsynced, renamed, and the parent directory
//!   fsynced, so a surviving file is never a partial one.
//!
//! # One writer per log
//!
//! [`writer::FsWriter::open`] locks **every** partition, so a second process
//! opening the same root fails rather than corrupting the offset sequence. This
//! is the constraint that makes offsets dense enough to use as cursors, and it
//! means a log has a single producer process.
//!
//! To spread production across processes, give each one a disjoint set with
//! [`writer::FsWriter::open_partitions_subset`] — but note that partition
//! assignment is derived from the event key, so a process can only accept
//! events that hash into the partitions it owns. Consumers have no such limit:
//! any number of processes share a log through
//! [`coordinator::FsPartitionCoordinator`].
//!
//! # Durability
//!
//! [`log::SyncPolicy`] decides when appends reach disk. The default,
//! [`log::SyncPolicy::EveryBytes`] at
//! [`log::DEFAULT_SYNC_INTERVAL_BYTES`], bounds what a power failure can cost
//! without paying an fsync per event. Use [`log::SyncPolicy::Always`] when no
//! acknowledged event may ever be lost, and [`log::SyncPolicy::Never`] only
//! when the log is reproducible from another source.
//!
//! Retention is caller-driven: [`writer::FsWriter::enforce_retention`] deletes
//! whole segments that a [`log::RetentionPolicy`] has aged or sized out, and
//! nothing calls it for you, so a policy without a caller never reclaims
//! anything. A consumer whose checkpoint falls behind
//! the retained range gets [`eventuary_core::Error::InvalidCursor`] rather than
//! a silent skip over the deleted events.
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
