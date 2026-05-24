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
    CoordinatedStream, CoordinatedSubscription,
};

use crate::partition_coordinator::SqlitePartitionCoordinator;
use crate::reader::{SqliteCursor, SqliteCursorAcker, SqliteReader, SqliteSubscription};

pub type SqliteCoordinatedReaderConfig = CoordinatedReaderConfig;
pub type SqliteCoordinatedSubscription = CoordinatedSubscription<SqliteSubscription, SqliteCursor>;
pub type SqliteCoordinatedReader = CoordinatedReader<SqliteReader, SqlitePartitionCoordinator>;
pub type SqliteCoordinatedAcker =
    CoordinatedAcker<SqliteCursorAcker, SqliteCursor, SqlitePartitionCoordinator>;
pub type SqliteCoordinatedCursor = CoordinatedCursor<SqliteCursor>;
pub type SqliteCoordinatedStream =
    CoordinatedStream<SqliteCursorAcker, SqliteCursor, SqlitePartitionCoordinator>;
