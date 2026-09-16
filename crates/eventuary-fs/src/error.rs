use std::io;
use std::path::Path;

use eventuary_core::Error;

pub(crate) fn store(context: impl AsRef<str>, error: impl std::fmt::Display) -> Error {
    Error::Store(format!("{}: {error}", context.as_ref()))
}

pub(crate) fn io_at(context: &str, path: &Path, error: io::Error) -> Error {
    Error::Store(format!("{context} {}: {error}", path.display()))
}

pub(crate) fn corrupt(path: &Path, detail: impl std::fmt::Display) -> Error {
    Error::Store(format!("corrupt log at {}: {detail}", path.display()))
}

pub(crate) fn join(error: impl std::fmt::Display) -> Error {
    Error::Store(format!("blocking task panicked: {error}"))
}
