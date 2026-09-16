use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use eventuary_core::io::Cursor;
use eventuary_core::io::OwnerId;
use eventuary_core::io::reader::{
    CheckpointScope, Generation, PartitionCoordinator, PartitionLease,
};
use eventuary_core::{Error, Partition, Result};
use fs4::fs_std::FileExt;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::atomic;
use crate::error::{io_at, join, store};
use crate::layout::encode_component;

const DIR: &str = "coordinator";
const CONSUMERS: &str = "consumers";
const PARTITIONS: &str = "partitions";
const LOCK: &str = ".lock";

#[derive(Debug, Clone, Default)]
pub struct FsPartitionCoordinatorConfig {
    pub dir: Option<PathBuf>,
}

#[derive(Serialize, Deserialize)]
struct ConsumerRecord {
    lease_until: DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
struct PartitionRecord<C> {
    partition_count: u32,
    owner_id: Option<String>,
    lease_until: Option<DateTime<Utc>>,
    generation: i64,
    checkpoint: Option<C>,
}

pub struct FsPartitionCoordinator<C> {
    dir: Arc<PathBuf>,
    _cursor: std::marker::PhantomData<fn() -> C>,
}

impl<C> Clone for FsPartitionCoordinator<C> {
    fn clone(&self) -> Self {
        Self {
            dir: Arc::clone(&self.dir),
            _cursor: std::marker::PhantomData,
        }
    }
}

impl<C> FsPartitionCoordinator<C> {
    pub fn open(root: impl AsRef<Path>, config: FsPartitionCoordinatorConfig) -> Result<Self> {
        let dir = config.dir.unwrap_or_else(|| root.as_ref().join(DIR));
        fs::create_dir_all(&dir).map_err(|e| io_at("create coordinator dir", &dir, e))?;
        Ok(Self {
            dir: Arc::new(dir),
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

    fn consumers_dir(&self, scope: &CheckpointScope) -> PathBuf {
        self.scope_dir(scope).join(CONSUMERS)
    }

    fn consumer_path(&self, scope: &CheckpointScope, owner_id: &OwnerId) -> PathBuf {
        self.consumers_dir(scope)
            .join(format!("{}.json", encode_component(owner_id.as_str())))
    }

    fn partitions_dir(&self, scope: &CheckpointScope) -> PathBuf {
        self.scope_dir(scope).join(PARTITIONS)
    }

    fn partition_path(&self, scope: &CheckpointScope, partition: Partition) -> PathBuf {
        self.partitions_dir(scope)
            .join(format!("{:05}.json", partition.id()))
    }

    fn partition_lock_path(&self, scope: &CheckpointScope, partition: Partition) -> PathBuf {
        self.partitions_dir(scope)
            .join(format!("{:05}{LOCK}", partition.id()))
    }
}

struct PartitionGuard {
    file: File,
}

impl PartitionGuard {
    fn acquire(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| io_at("create partitions dir", parent, e))?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path)
            .map_err(|e| io_at("open partition lock", path, e))?;
        FileExt::lock_exclusive(&file).map_err(|e| store("lock partition record", e))?;
        Ok(Self { file })
    }
}

impl Drop for PartitionGuard {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

fn read_partition<C: DeserializeOwned>(path: &Path) -> Result<Option<PartitionRecord<C>>> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| Error::Serialization(format!("partition record decode: {e}"))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(io_at("read partition record", path, e)),
    }
}

fn write_partition<C: Serialize>(path: &Path, record: &PartitionRecord<C>) -> Result<()> {
    let bytes = serde_json::to_vec(record)
        .map_err(|e| Error::Serialization(format!("partition record encode: {e}")))?;
    atomic::write(path, &bytes)
}

fn lease_until(lease_duration: Duration) -> Result<DateTime<Utc>> {
    Ok(Utc::now()
        + chrono::Duration::from_std(lease_duration)
            .map_err(|_| Error::Config("lease duration out of range".to_owned()))?)
}

fn count_mismatch(scope: &CheckpointScope, partition: Partition, stored: u32) -> Error {
    Error::Config(format!(
        "partition count mismatch for scope {} stream {} partition {}: stored {}, requested {}",
        scope.consumer_group_id.as_str(),
        scope.stream_id.as_str(),
        partition.id(),
        stored,
        partition.count(),
    ))
}

fn ownership_lost(partition: Partition, generation: Generation) -> Error {
    Error::OwnershipLost(format!(
        "partition {} generation {generation}",
        partition.id()
    ))
}

impl<C> PartitionCoordinator<C> for FsPartitionCoordinator<C>
where
    C: Cursor + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    async fn heartbeat(
        &self,
        scope: &CheckpointScope,
        owner_id: &OwnerId,
        lease_duration: Duration,
    ) -> Result<()> {
        let path = self.consumer_path(scope, owner_id);
        let until = lease_until(lease_duration)?;
        tokio::task::spawn_blocking(move || {
            let bytes = serde_json::to_vec(&ConsumerRecord { lease_until: until })
                .map_err(|e| Error::Serialization(format!("consumer encode: {e}")))?;
            atomic::write(&path, &bytes)
        })
        .await
        .map_err(join)?
    }

    async fn live_consumers(&self, scope: &CheckpointScope) -> Result<usize> {
        let dir = self.consumers_dir(scope);
        tokio::task::spawn_blocking(move || {
            let entries = match fs::read_dir(&dir) {
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
                Err(e) => return Err(io_at("read consumers dir", &dir, e)),
            };
            let now = Utc::now();
            let mut live = 0;
            for entry in entries {
                let entry = entry.map_err(|e| io_at("read consumer entry", &dir, e))?;
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) != Some("json") {
                    continue;
                }
                let Ok(bytes) = fs::read(&path) else { continue };
                let Ok(record) = serde_json::from_slice::<ConsumerRecord>(&bytes) else {
                    continue;
                };
                if record.lease_until > now {
                    live += 1;
                }
            }
            Ok(live)
        })
        .await
        .map_err(join)?
    }

    async fn release_consumer(&self, scope: &CheckpointScope, owner_id: &OwnerId) -> Result<()> {
        let path = self.consumer_path(scope, owner_id);
        tokio::task::spawn_blocking(move || match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io_at("remove consumer", &path, e)),
        })
        .await
        .map_err(join)?
    }

    async fn claim(
        &self,
        scope: &CheckpointScope,
        owner_id: &OwnerId,
        partition: Partition,
        lease_duration: Duration,
    ) -> Result<Option<PartitionLease<C>>> {
        let path = self.partition_path(scope, partition);
        let lock = self.partition_lock_path(scope, partition);
        let until = lease_until(lease_duration)?;
        let scope_owned = scope.clone();
        let owner_owned = owner_id.clone();

        tokio::task::spawn_blocking(move || {
            let _guard = PartitionGuard::acquire(&lock)?;
            let existing: Option<PartitionRecord<C>> = read_partition(&path)?;
            let now = Utc::now();

            let (generation, checkpoint) = match existing {
                None => (1, None),
                Some(record) => {
                    if record.partition_count != partition.count() {
                        return Err(count_mismatch(
                            &scope_owned,
                            partition,
                            record.partition_count,
                        ));
                    }
                    let takeable = record.owner_id.is_none()
                        || record.lease_until.is_none_or(|until| until < now)
                        || record.owner_id.as_deref() == Some(owner_owned.as_str());
                    if !takeable {
                        return Ok(None);
                    }
                    (record.generation + 1, record.checkpoint)
                }
            };

            let record = PartitionRecord {
                partition_count: partition.count(),
                owner_id: Some(owner_owned.as_str().to_owned()),
                lease_until: Some(until),
                generation,
                checkpoint: checkpoint.clone(),
            };
            write_partition(&path, &record)?;

            Ok(Some(PartitionLease {
                scope: scope_owned,
                owner_id: owner_owned,
                partition,
                generation: Generation::from_i64(generation),
                checkpoint_cursor: checkpoint,
                lease_until: until,
            }))
        })
        .await
        .map_err(join)?
    }

    async fn renew(&self, lease: &PartitionLease<C>, lease_duration: Duration) -> Result<()> {
        let path = self.partition_path(&lease.scope, lease.partition);
        let lock = self.partition_lock_path(&lease.scope, lease.partition);
        let until = lease_until(lease_duration)?;
        let scope_owned = lease.scope.clone();
        let owner = lease.owner_id.as_str().to_owned();
        let generation = lease.generation;
        let partition = lease.partition;

        tokio::task::spawn_blocking(move || {
            let _guard = PartitionGuard::acquire(&lock)?;
            let mut record: PartitionRecord<C> =
                read_partition(&path)?.ok_or_else(|| ownership_lost(partition, generation))?;
            if record.partition_count != partition.count() {
                return Err(count_mismatch(
                    &scope_owned,
                    partition,
                    record.partition_count,
                ));
            }
            if record.owner_id.as_deref() != Some(owner.as_str())
                || record.generation != generation.get()
            {
                return Err(ownership_lost(partition, generation));
            }
            record.lease_until = Some(until);
            write_partition(&path, &record)
        })
        .await
        .map_err(join)?
    }

    async fn release(&self, lease: &PartitionLease<C>) -> Result<()> {
        let path = self.partition_path(&lease.scope, lease.partition);
        let lock = self.partition_lock_path(&lease.scope, lease.partition);
        let scope_owned = lease.scope.clone();
        let owner = lease.owner_id.as_str().to_owned();
        let generation = lease.generation;
        let partition = lease.partition;

        tokio::task::spawn_blocking(move || {
            let _guard = PartitionGuard::acquire(&lock)?;
            let mut record: PartitionRecord<C> =
                read_partition(&path)?.ok_or_else(|| ownership_lost(partition, generation))?;
            if record.partition_count != partition.count() {
                return Err(count_mismatch(
                    &scope_owned,
                    partition,
                    record.partition_count,
                ));
            }
            if record.owner_id.as_deref() != Some(owner.as_str())
                || record.generation != generation.get()
            {
                return Err(ownership_lost(partition, generation));
            }
            record.owner_id = None;
            record.lease_until = None;
            record.generation += 1;
            write_partition(&path, &record)
        })
        .await
        .map_err(join)?
    }

    async fn checkpoint(&self, lease: &PartitionLease<C>, cursor: C) -> Result<()> {
        let path = self.partition_path(&lease.scope, lease.partition);
        let lock = self.partition_lock_path(&lease.scope, lease.partition);
        let scope_owned = lease.scope.clone();
        let owner = lease.owner_id.as_str().to_owned();
        let generation = lease.generation;
        let partition = lease.partition;

        tokio::task::spawn_blocking(move || {
            let _guard = PartitionGuard::acquire(&lock)?;
            let mut record: PartitionRecord<C> =
                read_partition(&path)?.ok_or_else(|| ownership_lost(partition, generation))?;
            if record.partition_count != partition.count() {
                return Err(count_mismatch(
                    &scope_owned,
                    partition,
                    record.partition_count,
                ));
            }
            if record.owner_id.as_deref() != Some(owner.as_str())
                || record.generation != generation.get()
            {
                return Err(Error::OwnershipLost(format!(
                    "checkpoint rejected for partition {}: stale owner/generation",
                    partition.id()
                )));
            }
            let advances = record
                .checkpoint
                .as_ref()
                .is_none_or(|stored| cursor.order_key() > stored.order_key());
            if advances {
                record.checkpoint = Some(cursor);
                write_partition(&path, &record)?;
            }
            Ok(())
        })
        .await
        .map_err(join)?
    }
}
