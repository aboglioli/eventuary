//! SQLite coordinated reader: thin alias over generic core type.

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
