use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use eventuary_core::io::reader::WatermarkStore;
use eventuary_core::{Error, Result};

use crate::atomic;
use crate::error::{io_at, join};
use crate::layout::encode_component;

const DIR: &str = "watermarks";

#[derive(Debug, Clone, Default)]
pub struct FsWatermarkStoreConfig {
    pub dir: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct FsWatermarkStore {
    dir: PathBuf,
}

impl FsWatermarkStore {
    pub fn open(root: impl AsRef<Path>, config: FsWatermarkStoreConfig) -> Result<Self> {
        let dir = config.dir.unwrap_or_else(|| root.as_ref().join(DIR));
        fs::create_dir_all(&dir).map_err(|e| io_at("create watermarks dir", &dir, e))?;
        Ok(Self { dir })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn path(&self, key: &str) -> PathBuf {
        self.dir.join(format!("{}.json", encode_component(key)))
    }
}

impl WatermarkStore for FsWatermarkStore {
    async fn load_watermark(&self, key: &str) -> Result<Option<DateTime<Utc>>> {
        let path = self.path(key);
        tokio::task::spawn_blocking(move || match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice::<DateTime<Utc>>(&bytes)
                .map(Some)
                .map_err(|e| Error::Serialization(format!("watermark decode: {e}"))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(io_at("read watermark", &path, e)),
        })
        .await
        .map_err(join)?
    }

    async fn save_watermark(&self, key: &str, ts: DateTime<Utc>) -> Result<()> {
        let path = self.path(key);
        tokio::task::spawn_blocking(move || {
            let bytes = serde_json::to_vec(&ts)
                .map_err(|e| Error::Serialization(format!("watermark encode: {e}")))?;
            atomic::write(&path, &bytes)
        })
        .await
        .map_err(join)?
    }
}
