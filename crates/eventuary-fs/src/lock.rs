use std::fs::{self, File, OpenOptions, TryLockError};
use std::path::Path;
use std::time::{Duration, Instant};

use eventuary_core::Result;

use crate::error::{contended, io_at, store};

pub const DEFAULT_LOCK_WAIT: Duration = Duration::from_secs(10);

const POLL: Duration = Duration::from_millis(5);

#[must_use = "dropping the lock releases the resource to other processes"]
pub(crate) struct FileLock {
    file: File,
}

impl FileLock {
    /// `flock` has no timed variant, so the bounded poll is what keeps contention from
    /// becoming a deadlock.
    pub(crate) fn acquire(path: &Path, resource: &str, wait: Duration) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| io_at("create lock dir", parent, e))?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path)
            .map_err(|e| io_at("open lock", path, e))?;

        let deadline = Instant::now() + wait;
        loop {
            match file.try_lock() {
                Ok(()) => return Ok(Self { file }),
                Err(TryLockError::WouldBlock) => {
                    let left = deadline.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        return Err(contended(format!("{resource} is held by another process")));
                    }
                    std::thread::sleep(POLL.min(left));
                }
                Err(TryLockError::Error(e)) => return Err(store(format!("lock {resource}"), e)),
            }
        }
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}
