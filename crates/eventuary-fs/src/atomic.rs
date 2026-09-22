use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use eventuary_core::Result;

use crate::error::io_at;

/// Writes `bytes` to `path` by renaming a temporary file over it, so a reader sees either
/// the previous contents or the new ones.
///
/// The temporary name is unique per call. Deriving it from the target alone lets two
/// processes writing the same file share one temporary path, where `File::create` truncates
/// what the other is still writing and the rename then publishes a torn file.
pub(crate) fn write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent();
    if let Some(parent) = parent {
        fs::create_dir_all(parent).map_err(|e| io_at("create dir", parent, e))?;
    }
    let tmp = temp_path(path);
    {
        let mut file = File::create(&tmp).map_err(|e| io_at("create temp file", &tmp, e))?;
        file.write_all(bytes)
            .map_err(|e| io_at("write temp file", &tmp, e))?;
        file.sync_all()
            .map_err(|e| io_at("sync temp file", &tmp, e))?;
    }
    if let Err(e) = fs::rename(&tmp, path) {
        let _ = fs::remove_file(&tmp);
        return Err(io_at("rename temp file", &tmp, e));
    }
    if let Some(parent) = parent {
        sync_dir(parent)?;
    }
    Ok(())
}

fn temp_path(path: &Path) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);

    let nonce = NEXT.fetch_add(1, Ordering::Relaxed);
    let name = path.file_name().map(|n| n.to_string_lossy().into_owned());
    let stem = name.unwrap_or_else(|| "file".to_owned());
    path.with_file_name(format!(".{stem}.{}.{nonce}.tmp", std::process::id()))
}

/// Syncing the file persists its contents; only syncing the directory persists
/// the rename, without which the write can revert to its previous version.
#[cfg(unix)]
fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)
        .and_then(|handle| handle.sync_all())
        .map_err(|e| io_at("sync dir", dir, e))
}

#[cfg(not(unix))]
fn sync_dir(_dir: &Path) -> Result<()> {
    Ok(())
}
