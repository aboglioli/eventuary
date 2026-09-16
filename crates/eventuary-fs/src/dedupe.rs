use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};

use eventuary_core::io::reader::DedupeStore;
use eventuary_core::{Event, Result};

use crate::error::{io_at, join};

const DIR: &str = "dedupe";

#[derive(Debug, Clone, Default)]
pub struct FsDedupeStoreConfig {
    pub dir: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct FsDedupeStore {
    dir: PathBuf,
}

impl FsDedupeStore {
    pub fn open(root: impl AsRef<Path>, config: FsDedupeStoreConfig) -> Result<Self> {
        let dir = config.dir.unwrap_or_else(|| root.as_ref().join(DIR));
        fs::create_dir_all(&dir).map_err(|e| io_at("create dedupe dir", &dir, e))?;
        Ok(Self { dir })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn path(&self, event_id: &str) -> PathBuf {
        let (prefix, rest) = event_id.split_at(2.min(event_id.len()));
        self.dir.join(prefix).join(rest)
    }
}

impl DedupeStore for FsDedupeStore {
    async fn exists(&self, event: &Event) -> Result<bool> {
        let path = self.path(&event.id().as_uuid().to_string());
        tokio::task::spawn_blocking(move || Ok(path.exists()))
            .await
            .map_err(join)?
    }

    async fn mark_processed(&self, event: &Event) -> Result<()> {
        let path = self.path(&event.id().as_uuid().to_string());
        tokio::task::spawn_blocking(move || create_marker(&path).map(|_| ()))
            .await
            .map_err(join)?
    }

    async fn mark_if_new(&self, event: &Event) -> Result<bool> {
        let path = self.path(&event.id().as_uuid().to_string());
        tokio::task::spawn_blocking(move || create_marker(&path))
            .await
            .map_err(join)?
    }
}

fn create_marker(path: &Path) -> Result<bool> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| io_at("create dedupe shard", parent, e))?;
    }
    match OpenOptions::new().create_new(true).write(true).open(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(e) => Err(io_at("create dedupe marker", path, e)),
    }
}
