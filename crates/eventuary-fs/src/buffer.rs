use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use eventuary_core::io::reader::{BufferEntry, BufferStore};
use eventuary_core::{Error, Event, Payload, Result, SerializedEvent};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::atomic;
use crate::error::{io_at, join};

const DIR: &str = "buffer";

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct FsBufferStoreId(pub u64);

#[derive(Debug, Clone, Default)]
pub struct FsBufferStoreConfig {
    pub dir: Option<PathBuf>,
}

#[derive(Serialize, Deserialize)]
struct BufferedEntry<C> {
    id: u64,
    event: SerializedEvent,
    cursor: C,
}

pub struct FsBufferStore<C> {
    dir: Arc<PathBuf>,
    next_id: Arc<AtomicU64>,
    _cursor: std::marker::PhantomData<fn() -> C>,
}

impl<C> Clone for FsBufferStore<C> {
    fn clone(&self) -> Self {
        Self {
            dir: Arc::clone(&self.dir),
            next_id: Arc::clone(&self.next_id),
            _cursor: std::marker::PhantomData,
        }
    }
}

impl<C> FsBufferStore<C> {
    pub fn open(root: impl AsRef<Path>, config: FsBufferStoreConfig) -> Result<Self> {
        let dir = config.dir.unwrap_or_else(|| root.as_ref().join(DIR));
        fs::create_dir_all(&dir).map_err(|e| io_at("create buffer dir", &dir, e))?;
        let next_id = highest_id(&dir)?.map(|id| id + 1).unwrap_or(0);
        Ok(Self {
            dir: Arc::new(dir),
            next_id: Arc::new(AtomicU64::new(next_id)),
            _cursor: std::marker::PhantomData,
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn path(&self, id: u64) -> PathBuf {
        self.dir.join(format!("{id:020}.json"))
    }
}

impl<C> BufferStore<C, Payload> for FsBufferStore<C>
where
    C: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    type Id = FsBufferStoreId;

    async fn push(&self, event: &Event, cursor: &C) -> Result<Self::Id> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let path = self.path(id);
        let entry = BufferedEntry {
            id,
            event: SerializedEvent::from_event(event)?,
            cursor: cursor.clone(),
        };
        tokio::task::spawn_blocking(move || {
            let bytes = serde_json::to_vec(&entry)
                .map_err(|e| Error::Serialization(format!("buffer encode: {e}")))?;
            atomic::write(&path, &bytes)
        })
        .await
        .map_err(join)??;
        Ok(FsBufferStoreId(id))
    }

    async fn pending(&self) -> Result<Vec<BufferEntry<C, Self::Id, Payload>>> {
        let dir = Arc::clone(&self.dir);
        tokio::task::spawn_blocking(move || {
            let mut paths = entry_paths(&dir)?;
            paths.sort();
            let mut out = Vec::with_capacity(paths.len());
            for path in paths {
                let bytes = match fs::read(&path) {
                    Ok(b) => b,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(io_at("read buffer entry", &path, e)),
                };
                let entry: BufferedEntry<C> = serde_json::from_slice(&bytes)
                    .map_err(|e| Error::Serialization(format!("buffer decode: {e}")))?;
                out.push(BufferEntry {
                    id: FsBufferStoreId(entry.id),
                    event: entry.event.to_event()?,
                    cursor: entry.cursor,
                });
            }
            Ok(out)
        })
        .await
        .map_err(join)?
    }

    async fn ack(&self, id: &Self::Id) -> Result<()> {
        let path = self.path(id.0);
        tokio::task::spawn_blocking(move || match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io_at("remove buffer entry", &path, e)),
        })
        .await
        .map_err(join)?
    }

    async fn nack(&self, _id: &Self::Id) -> Result<()> {
        Ok(())
    }
}

fn entry_paths(dir: &Path) -> Result<Vec<PathBuf>> {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(io_at("read buffer dir", dir, e)),
    };
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| io_at("read buffer entry", dir, e))?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("json") {
            paths.push(path);
        }
    }
    Ok(paths)
}

fn highest_id(dir: &Path) -> Result<Option<u64>> {
    Ok(entry_paths(dir)?
        .iter()
        .filter_map(|p| p.file_stem()?.to_str()?.parse::<u64>().ok())
        .max())
}
