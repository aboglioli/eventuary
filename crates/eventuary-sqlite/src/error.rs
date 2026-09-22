use std::fmt;
use std::sync::PoisonError;

use eventuary_core::Error;
use rusqlite::ErrorCode;

/// Converts a driver error, reporting the codes SQLite uses for "another connection holds
/// what I need" as [`Error::Contended`] so a caller can retry those and only those.
pub(crate) fn store(error: rusqlite::Error) -> Error {
    if let rusqlite::Error::SqliteFailure(failure, _) = &error
        && matches!(
            failure.code,
            ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked
        )
    {
        return Error::Contended(error.to_string());
    }
    Error::Store(error.to_string())
}

/// A poisoned connection mutex means another thread panicked mid-statement, which is a
/// defect rather than contention.
pub(crate) fn poisoned<T>(error: PoisonError<T>) -> Error {
    Error::Store(format!("sqlite connection poisoned: {error}"))
}

pub(crate) fn join(error: impl fmt::Display) -> Error {
    Error::Store(format!("blocking task panicked: {error}"))
}
