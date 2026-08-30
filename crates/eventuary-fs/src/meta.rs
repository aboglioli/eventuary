use std::fs;
use std::num::NonZeroU32;
use std::path::Path;

use eventuary_core::{Error, Result};
use serde::{Deserialize, Serialize};

use crate::error::io_at;
use crate::layout::meta_path;

const FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct LogMeta {
    pub(crate) format_version: u32,
    pub(crate) partition_count: u32,
}

impl LogMeta {
    pub(crate) fn new(partition_count: NonZeroU32) -> Self {
        Self {
            format_version: FORMAT_VERSION,
            partition_count: partition_count.get(),
        }
    }

    pub(crate) fn load(root: &Path) -> Result<Option<Self>> {
        let path = meta_path(root);
        match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|e| Error::Serialization(format!("decode {}: {e}", path.display()))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(io_at("read meta", &path, e)),
        }
    }

    pub(crate) fn store(&self, root: &Path) -> Result<()> {
        let path = meta_path(root);
        let bytes = serde_json::to_vec_pretty(self)
            .map_err(|e| Error::Serialization(format!("encode meta: {e}")))?;
        crate::atomic::write(&path, &bytes)
    }

    pub(crate) fn ensure(root: &Path, partition_count: NonZeroU32) -> Result<Self> {
        match Self::load(root)? {
            Some(existing) => {
                if existing.partition_count != partition_count.get() {
                    return Err(Error::Config(format!(
                        "log at {} was created with {} partitions, refusing to open with {}",
                        root.display(),
                        existing.partition_count,
                        partition_count.get()
                    )));
                }
                if existing.format_version != FORMAT_VERSION {
                    return Err(Error::Config(format!(
                        "log at {} uses format version {}, this build supports {FORMAT_VERSION}",
                        root.display(),
                        existing.format_version
                    )));
                }
                Ok(existing)
            }
            None => {
                let meta = Self::new(partition_count);
                meta.store(root)?;
                Ok(meta)
            }
        }
    }
}
