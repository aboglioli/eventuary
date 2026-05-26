//! SQLite coordinated reader: thin alias over the generic core type.
//!
//! Why this file is alias-only:
//!
//! `CoordinatedReader<R, Coord>` is fully generic in `eventuary-core` — it
//! composes any partition-aware `Reader<R>` with any `PartitionCoordinator<C>`
//! and contains no SQLite-specific code. Every backend therefore only needs
//! `pub type` aliases to specialize the generic with its own reader and
//! coordinator (`SqliteReader` + `SqlitePartitionCoordinator`). The same
//! module path shape (`eventuary::sqlite::coordinated_reader::*`) is
//! preserved across backends so users learn one structure once.
//!
//! These aliases shorten call sites — `SqliteCoordinatedReader` reads better
//! than `CoordinatedReader<SqliteReader, SqlitePartitionCoordinator>` — and
//! give the umbrella a canonical, discoverable path next to `reader`,
//! `writer`, and `partition_coordinator` modules.

use eventuary_core::io::reader::{
    CoordinatedAcker, CoordinatedCursor, CoordinatedReader, CoordinatedReaderConfig,
    CoordinatedStream, CoordinatedSubscription, PartitionAcker, PartitionedCoordAdapter,
    PartitionedCursor,
};

use crate::partition_coordinator::SqlitePartitionCoordinator;
use crate::reader::{SqliteCursor, SqliteCursorAcker, SqliteReader, SqliteSubscription};

pub type SqliteCoordinatedReaderConfig = CoordinatedReaderConfig;
pub type SqliteCoordinatedSubscription = CoordinatedSubscription<SqliteSubscription, SqliteCursor>;
pub type SqliteCoordinatedReader = CoordinatedReader<SqliteReader, SqlitePartitionCoordinator>;
/// Standalone `PartitionLease`-fenced acker over the raw `SqliteCursor`.
/// This alias matches the simple shape used by code paths that wire a
/// coordinator outside of `CoordinatedReader::read`. The stream-emitted
/// acker after the shared-fetch rewrite is [`SqliteCoordinatedStreamAcker`].
pub type SqliteCoordinatedAcker =
    CoordinatedAcker<SqliteCursorAcker, SqliteCursor, SqlitePartitionCoordinator>;
/// Acker carried on every message emitted by [`SqliteCoordinatedReader`].
pub type SqliteCoordinatedStreamAcker = CoordinatedAcker<
    PartitionAcker<SqliteCursorAcker, SqliteCursor>,
    PartitionedCursor<SqliteCursor>,
    PartitionedCoordAdapter<SqlitePartitionCoordinator, SqliteCursor>,
>;
pub type SqliteCoordinatedCursor = CoordinatedCursor<PartitionedCursor<SqliteCursor>>;
pub type SqliteCoordinatedStream = CoordinatedStream<
    PartitionAcker<SqliteCursorAcker, SqliteCursor>,
    PartitionedCursor<SqliteCursor>,
    PartitionedCoordAdapter<SqlitePartitionCoordinator, SqliteCursor>,
>;
