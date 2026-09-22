use std::fmt;
use std::sync::PoisonError;

use eventuary_core::Error;
use rusqlite::ErrorCode;

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

/// A poisoned mutex means another thread panicked mid-statement: a defect, not contention,
/// so it must not be retried.
pub(crate) fn poisoned<T>(error: PoisonError<T>) -> Error {
    Error::Store(format!("sqlite connection poisoned: {error}"))
}

pub(crate) fn join(error: impl fmt::Display) -> Error {
    Error::Store(format!("blocking task panicked: {error}"))
}
