use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use eventuary_core::Result;

use crate::error::io_at;

pub(crate) fn write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent();
    if let Some(parent) = parent {
        fs::create_dir_all(parent).map_err(|e| io_at("create dir", parent, e))?;
    }
    let tmp = path.with_extension("tmp");
    {
        let mut file = File::create(&tmp).map_err(|e| io_at("create temp file", &tmp, e))?;
        file.write_all(bytes)
            .map_err(|e| io_at("write temp file", &tmp, e))?;
        file.sync_all()
            .map_err(|e| io_at("sync temp file", &tmp, e))?;
    }
    fs::rename(&tmp, path).map_err(|e| io_at("rename temp file", &tmp, e))?;
    if let Some(parent) = parent {
        sync_dir(parent)?;
    }
    Ok(())
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
