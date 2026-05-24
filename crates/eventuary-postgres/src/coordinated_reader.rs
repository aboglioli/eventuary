//! Postgres coordinated reader: thin alias over generic core type.

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
