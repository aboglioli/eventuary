use eventuary_core::Error;

/// PostgreSQL codes that mean "another transaction holds what I need, try again": a
/// serialization failure, a detected deadlock, and a lock that `NOWAIT` refused to wait for.
const CONTENDED: [&str; 3] = ["40001", "40P01", "55P03"];

pub(crate) fn store(error: sqlx::Error) -> Error {
    if let sqlx::Error::Database(db) = &error
        && db.code().is_some_and(|code| CONTENDED.contains(&&*code))
    {
        return Error::Contended(error.to_string());
    }
    Error::Store(error.to_string())
}
