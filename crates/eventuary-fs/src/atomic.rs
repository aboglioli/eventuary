use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use eventuary_core::Result;

use crate::error::io_at;

pub(crate) fn write(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
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
    fs::rename(&tmp, path).map_err(|e| io_at("rename temp file", &tmp, e))
}
