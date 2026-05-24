//! Postgres coordinated reader: thin alias over the generic core type.
//!
//! Why this file is alias-only:
//!
//! `CoordinatedReader<R, Coord>` is fully generic in `eventuary-core` — it
//! composes any partition-aware `Reader<R>` with any `PartitionCoordinator<C>`
//! and contains no Postgres-specific code. Every backend therefore only needs
//! `pub type` aliases to specialize the generic with its own reader and
//! coordinator (`PgReader` + `PgPartitionCoordinator`). The same module path
//! shape (`eventuary::postgres::coordinated_reader::*`) is preserved across
//! backends so users learn one structure once.
//!
//! These aliases shorten call sites — `PgCoordinatedReader` reads better than
//! `CoordinatedReader<PgReader, PgPartitionCoordinator>` — and give the
//! umbrella a canonical, discoverable path next to `reader`, `writer`, and
//! `partition_coordinator` modules.

use eventuary_core::io::reader::{
    CoordinatedAcker, CoordinatedCursor, CoordinatedReader, CoordinatedReaderConfig,
    CoordinatedStream, CoordinatedSubscription,
};

use crate::partition_coordinator::PgPartitionCoordinator;
use crate::reader::{PgCursor, PgCursorAcker, PgReader, PgSubscription};

pub type PgCoordinatedReaderConfig = CoordinatedReaderConfig;
pub type PgCoordinatedSubscription = CoordinatedSubscription<PgSubscription, PgCursor>;
pub type PgCoordinatedReader = CoordinatedReader<PgReader, PgPartitionCoordinator>;
pub type PgCoordinatedAcker = CoordinatedAcker<PgCursorAcker, PgCursor, PgPartitionCoordinator>;
pub type PgCoordinatedCursor = CoordinatedCursor<PgCursor>;
pub type PgCoordinatedStream = CoordinatedStream<PgCursorAcker, PgCursor, PgPartitionCoordinator>;
