use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use eventuary_core::io::Writer;
use eventuary_core::partition::{
    Fnv1a64PartitionHasher, Partition, PartitionHasher, PartitionKeyResolver,
};
use eventuary_core::{Error, Event, Result, SerializedEvent};
use tokio::sync::Mutex;

use crate::error::join;
use crate::log::{LogConfig, PartitionLog};
use crate::meta::LogMeta;

#[derive(Clone, Default)]
pub enum FsPartitioningConfig {
    #[default]
    Single,
    Inline {
        partition_count: NonZeroU32,
        key_resolver: Arc<dyn PartitionKeyResolver>,
        hasher: Arc<dyn PartitionHasher>,
    },
}

impl FsPartitioningConfig {
    pub fn inline(
        count: NonZeroU32,
        resolver: impl PartitionKeyResolver + 'static,
        hasher: impl PartitionHasher + 'static,
    ) -> Self {
        Self::Inline {
            partition_count: count,
            key_resolver: Arc::new(resolver),
            hasher: Arc::new(hasher),
        }
    }

    pub fn by_event_key(count: NonZeroU32) -> Self {
        Self::inline(
            count,
            eventuary_core::partition::EventKeyPartitionKeyResolver::new(),
            Fnv1a64PartitionHasher,
        )
    }

    pub fn partition_count(&self) -> NonZeroU32 {
        match self {
            Self::Single => NonZeroU32::new(1).expect("one is non-zero"),
            Self::Inline {
                partition_count, ..
            } => *partition_count,
        }
    }

    fn partition_for(&self, event: &Event) -> Result<Partition> {
        match self {
            Self::Single => Partition::new(0, self.partition_count()),
            Self::Inline {
                partition_count,
                key_resolver,
                hasher,
            } => {
                let key = key_resolver.partition_key(event)?;
                Ok(hasher.partition_for(&key, *partition_count))
            }
        }
    }
}

impl fmt::Debug for FsPartitioningConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Single => write!(f, "FsPartitioningConfig::Single"),
            Self::Inline {
                partition_count, ..
            } => f
                .debug_struct("FsPartitioningConfig::Inline")
                .field("partition_count", partition_count)
                .finish(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct FsWriterConfig {
    pub log: LogConfig,
    pub partitioning: FsPartitioningConfig,
}

pub struct FsWriter {
    root: PathBuf,
    partitioning: FsPartitioningConfig,
    partitions: BTreeMap<u32, Arc<Mutex<PartitionLog>>>,
}

impl FsWriter {
    pub fn open(root: impl Into<PathBuf>, config: FsWriterConfig) -> Result<Self> {
        let root = root.into();
        let count = config.partitioning.partition_count();
        LogMeta::ensure(&root, count)?;
        Self::open_partitions(root, config, (0..count.get()).collect())
    }

    pub fn open_partitions_subset(
        root: impl Into<PathBuf>,
        config: FsWriterConfig,
        owned: Vec<u32>,
    ) -> Result<Self> {
        let root = root.into();
        let count = config.partitioning.partition_count();
        LogMeta::ensure(&root, count)?;
        for id in &owned {
            if *id >= count.get() {
                return Err(Error::Config(format!(
                    "partition {id} out of range for count {}",
                    count.get()
                )));
            }
        }
        Self::open_partitions(root, config, owned)
    }

    fn open_partitions(root: PathBuf, config: FsWriterConfig, owned: Vec<u32>) -> Result<Self> {
        let mut partitions = BTreeMap::new();
        for id in owned {
            let log = PartitionLog::open_writable(&root, id, config.log)?;
            partitions.insert(id, Arc::new(Mutex::new(log)));
        }
        Ok(Self {
            root,
            partitioning: config.partitioning,
            partitions,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn partition_count(&self) -> NonZeroU32 {
        self.partitioning.partition_count()
    }

    pub fn owned_partitions(&self) -> Vec<u32> {
        self.partitions.keys().copied().collect()
    }

    pub async fn next_offset(&self, partition_id: u32) -> Result<u64> {
        let log = Arc::clone(self.partition(partition_id)?);
        tokio::task::spawn_blocking(move || Ok(log.blocking_lock().next_offset()))
            .await
            .map_err(join)?
    }

    pub async fn sync(&self) -> Result<()> {
        for log in self.partitions.values() {
            let log = Arc::clone(log);
            tokio::task::spawn_blocking(move || log.blocking_lock().sync())
                .await
                .map_err(join)??;
        }
        Ok(())
    }

    pub async fn enforce_retention(&self) -> Result<usize> {
        let mut removed = 0;
        for log in self.partitions.values() {
            let log = Arc::clone(log);
            removed += tokio::task::spawn_blocking(move || log.blocking_lock().enforce_retention())
                .await
                .map_err(join)??;
        }
        Ok(removed)
    }

    fn partition(&self, partition_id: u32) -> Result<&Arc<Mutex<PartitionLog>>> {
        self.partitions.get(&partition_id).ok_or_else(|| {
            Error::Config(format!(
                "partition {partition_id} is not owned by this writer"
            ))
        })
    }

    fn route(&self, event: &Event) -> Result<(u32, Arc<Mutex<PartitionLog>>)> {
        let partition = self.partitioning.partition_for(event)?;
        let log = self.partition(partition.id())?;
        Ok((partition.id(), Arc::clone(log)))
    }
}

impl Writer for FsWriter {
    async fn write(&self, event: &Event) -> Result<()> {
        let (_, log) = self.route(event)?;
        let serialized = SerializedEvent::from_event(event)?;
        tokio::task::spawn_blocking(move || {
            let mut guard = log.blocking_lock();
            guard.append(serialized).map(|_| ())
        })
        .await
        .map_err(join)?
    }

    async fn write_all(&self, events: &[Event]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let mut batches: BTreeMap<u32, (Arc<Mutex<PartitionLog>>, Vec<SerializedEvent>)> =
            BTreeMap::new();
        for event in events {
            let (id, log) = self.route(event)?;
            batches
                .entry(id)
                .or_insert_with(|| (log, Vec::new()))
                .1
                .push(SerializedEvent::from_event(event)?);
        }
        for (_, (log, serialized)) in batches {
            tokio::task::spawn_blocking(move || {
                let mut guard = log.blocking_lock();
                guard.append_all(serialized).map(|_| ())
            })
            .await
            .map_err(join)??;
        }
        Ok(())
    }
}
