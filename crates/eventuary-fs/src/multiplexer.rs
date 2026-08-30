use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};

use eventuary_core::Result;
use eventuary_core::io::handler::{MultiplexerKey, MultiplexerStore};

use crate::error::{io_at, join};
use crate::layout::encode_component;

const DIR: &str = "multiplexer";

#[derive(Debug, Clone, Default)]
pub struct FsMultiplexerStoreConfig {
    pub dir: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct FsMultiplexerStore {
    dir: PathBuf,
}

impl FsMultiplexerStore {
    pub fn open(root: impl AsRef<Path>, config: FsMultiplexerStoreConfig) -> Result<Self> {
        let dir = config.dir.unwrap_or_else(|| root.as_ref().join(DIR));
        fs::create_dir_all(&dir).map_err(|e| io_at("create multiplexer dir", &dir, e))?;
        Ok(Self { dir })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn path(&self, key: &MultiplexerKey) -> PathBuf {
        self.dir
            .join(encode_component(key.subscriber_id.as_str()))
            .join(key.event_id.as_uuid().to_string())
    }
}

impl MultiplexerStore for FsMultiplexerStore {
    async fn is_completed(&self, key: &MultiplexerKey) -> Result<bool> {
        let path = self.path(key);
        tokio::task::spawn_blocking(move || Ok(path.exists()))
            .await
            .map_err(join)?
    }

    async fn mark_completed(&self, key: &MultiplexerKey) -> Result<()> {
        let path = self.path(key);
        tokio::task::spawn_blocking(move || {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)
                    .map_err(|e| io_at("create subscriber dir", parent, e))?;
            }
            match OpenOptions::new().create_new(true).write(true).open(&path) {
                Ok(_) => Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
                Err(e) => Err(io_at("mark completed", &path, e)),
            }
        })
        .await
        .map_err(join)?
    }
}
