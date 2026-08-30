use std::fs;
use std::path::{Path, PathBuf};

use eventuary_core::io::reader::{CheckpointKey, CheckpointScope, CheckpointStore};
use eventuary_core::io::{Cursor, CursorId};
use eventuary_core::{Error, Result};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::atomic;
use crate::error::io_at;
use crate::layout::{checkpoints_dir, decode_component, encode_component};

const EXTENSION: &str = "json";

#[derive(Debug, Clone, Default)]
pub struct FsCheckpointStoreConfig {
    pub dir: Option<PathBuf>,
}

pub struct FsCheckpointStore<C> {
    dir: PathBuf,
    _cursor: std::marker::PhantomData<fn() -> C>,
}

impl<C> Clone for FsCheckpointStore<C> {
    fn clone(&self) -> Self {
        Self {
            dir: self.dir.clone(),
            _cursor: std::marker::PhantomData,
        }
    }
}

impl<C> FsCheckpointStore<C> {
    pub fn open(root: impl AsRef<Path>, config: FsCheckpointStoreConfig) -> Result<Self> {
        let dir = config.dir.unwrap_or_else(|| checkpoints_dir(root.as_ref()));
        fs::create_dir_all(&dir).map_err(|e| io_at("create checkpoints dir", &dir, e))?;
        Ok(Self {
            dir,
            _cursor: std::marker::PhantomData,
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn scope_dir(&self, scope: &CheckpointScope) -> PathBuf {
        self.dir
            .join(encode_component(scope.consumer_group_id.as_str()))
            .join(encode_component(scope.stream_id.as_str()))
    }

    fn key_path(&self, key: &CheckpointKey) -> PathBuf {
        self.scope_dir(&key.scope).join(format!(
            "{}.{EXTENSION}",
            encode_component(key.cursor_id.as_str())
        ))
    }
}

impl<C> CheckpointStore<C> for FsCheckpointStore<C>
where
    C: Cursor + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    async fn load(&self, key: &CheckpointKey) -> Result<Option<C>> {
        let path = self.key_path(key);
        tokio::task::spawn_blocking(move || read_cursor(&path))
            .await
            .map_err(crate::error::join)?
    }

    async fn load_scope(&self, scope: &CheckpointScope) -> Result<Vec<(CursorId, C)>> {
        let dir = self.scope_dir(scope);
        tokio::task::spawn_blocking(move || {
            let entries = match fs::read_dir(&dir) {
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
                Err(e) => return Err(io_at("read checkpoint scope", &dir, e)),
            };
            let mut out = Vec::new();
            for entry in entries {
                let entry = entry.map_err(|e| io_at("read checkpoint entry", &dir, e))?;
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) != Some(EXTENSION) {
                    continue;
                }
                let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                    continue;
                };
                let Some(decoded) = decode_component(stem) else {
                    continue;
                };
                let Ok(cursor_id) = CursorId::new(decoded) else {
                    continue;
                };
                if let Some(cursor) = read_cursor::<C>(&path)? {
                    out.push((cursor_id, cursor));
                }
            }
            out.sort_by(|a, b| a.0.cmp(&b.0));
            Ok(out)
        })
        .await
        .map_err(crate::error::join)?
    }

    async fn commit(&self, key: &CheckpointKey, cursor: C) -> Result<()> {
        let path = self.key_path(key);
        tokio::task::spawn_blocking(move || {
            let bytes = serde_json::to_vec(&cursor)
                .map_err(|e| Error::Serialization(format!("checkpoint encode: {e}")))?;
            atomic::write(&path, &bytes)
        })
        .await
        .map_err(crate::error::join)?
    }
}

fn read_cursor<C: DeserializeOwned>(path: &Path) -> Result<Option<C>> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| Error::Serialization(format!("checkpoint decode: {e}"))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(io_at("read checkpoint", path, e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_and_decodes_cursor_ids_with_separators() {
        let encoded = encode_component("partition:64:17");

        assert_eq!(encoded, "partition%3A64%3A17");
        assert_eq!(decode_component(&encoded).unwrap(), "partition:64:17");
    }

    #[test]
    fn plain_ids_round_trip_unchanged() {
        assert_eq!(encode_component("global"), "global");
        assert_eq!(decode_component("global").unwrap(), "global");
    }

    #[test]
    fn distinct_ids_never_collide_after_encoding() {
        assert_ne!(encode_component("a:b"), encode_component("a_b"));
    }
}
