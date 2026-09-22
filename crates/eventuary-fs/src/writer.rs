use std::collections::{BTreeMap, BTreeSet};
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
use crate::log::{LogConfig, PartitionLog, WriterAccess};
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
    config: LogConfig,
    partitioning: FsPartitioningConfig,
    owned: BTreeSet<u32>,
    /// What `sync` and `enforce_retention` act on: an untouched partition has nothing to
    /// flush and opening one would take its lock for no reason.
    touched: Mutex<BTreeSet<u32>>,
    /// Empty under [`WriterAccess::Shared`], which keeps no state between writes.
    held: Mutex<BTreeMap<u32, Arc<Mutex<PartitionLog>>>>,
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
        Ok(Self {
            root,
            config: config.log,
            partitioning: config.partitioning,
            owned: owned.into_iter().collect(),
            touched: Mutex::new(BTreeSet::new()),
            held: Mutex::new(BTreeMap::new()),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn partition_count(&self) -> NonZeroU32 {
        self.partitioning.partition_count()
    }

    pub fn owned_partitions(&self) -> Vec<u32> {
        self.owned.iter().copied().collect()
    }

    /// Reads the partition tail without taking its lock, so asking does not claim the
    /// partition or disturb the writer that holds it. The answer is a snapshot: under
    /// [`WriterAccess::Shared`] another process may append before you use it.
    pub async fn next_offset(&self, partition_id: u32) -> Result<u64> {
        self.ensure_owned(partition_id)?;
        let (root, config) = (self.root.clone(), self.config);
        tokio::task::spawn_blocking(move || {
            Ok(PartitionLog::open_readonly(&root, partition_id, config)?.next_offset())
        })
        .await
        .map_err(join)?
    }

    pub async fn sync(&self) -> Result<()> {
        for id in self.touched_partitions().await {
            self.with_log(id, |log| log.sync()).await?;
        }
        Ok(())
    }

    pub async fn enforce_retention(&self) -> Result<usize> {
        let mut removed = 0;
        for id in self.touched_partitions().await {
            removed += self.with_log(id, |log| log.enforce_retention()).await?;
        }
        Ok(removed)
    }

    fn ensure_owned(&self, partition_id: u32) -> Result<()> {
        if self.owned.contains(&partition_id) {
            return Ok(());
        }
        Err(Error::Config(format!(
            "partition {partition_id} is not owned by this writer"
        )))
    }

    async fn touched_partitions(&self) -> Vec<u32> {
        self.touched.lock().await.iter().copied().collect()
    }

    /// Runs `op` against a partition while its lock is held, and — under
    /// [`WriterAccess::Shared`] — only while it is held. The mode decides the lock's
    /// lifetime, so no call site can append outside the lock or hold a partition longer
    /// than its mode allows.
    async fn with_log<T, F>(&self, partition_id: u32, op: F) -> Result<T>
    where
        F: FnOnce(&mut PartitionLog) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        self.ensure_owned(partition_id)?;
        self.touched.lock().await.insert(partition_id);

        match self.config.access {
            WriterAccess::Shared => {
                let (root, config) = (self.root.clone(), self.config);
                tokio::task::spawn_blocking(move || {
                    let mut log = PartitionLog::open_writable(&root, partition_id, config)?;
                    op(&mut log)
                })
                .await
                .map_err(join)?
            }
            WriterAccess::Exclusive => {
                let log = self.hold(partition_id).await?;
                tokio::task::spawn_blocking(move || op(&mut log.blocking_lock()))
                    .await
                    .map_err(join)?
            }
        }
    }

    /// Opens a partition and keeps it, for [`WriterAccess::Exclusive`]. The acquisition
    /// happens outside the `held` lock, so waiting for one partition cannot stall writes to
    /// every other partition in this process.
    async fn hold(&self, partition_id: u32) -> Result<Arc<Mutex<PartitionLog>>> {
        if let Some(log) = self.held.lock().await.get(&partition_id) {
            return Ok(Arc::clone(log));
        }

        let (root, config) = (self.root.clone(), self.config);
        let opened = tokio::task::spawn_blocking(move || {
            PartitionLog::open_writable(&root, partition_id, config)
        })
        .await
        .map_err(join)?;

        let mut held = self.held.lock().await;
        if let Some(log) = held.get(&partition_id) {
            return Ok(Arc::clone(log));
        }
        let log = Arc::new(Mutex::new(opened?));
        held.insert(partition_id, Arc::clone(&log));
        Ok(log)
    }

    /// Groups a batch by partition, checking ownership and serializing before any lock is
    /// taken, so a batch naming an unowned partition fails having written nothing. Ordering
    /// by partition id also fixes the order locks are taken in, which is what keeps two
    /// [`WriterAccess::Exclusive`] writers from each holding what the other needs.
    fn group(&self, events: &[Event]) -> Result<BTreeMap<u32, Vec<SerializedEvent>>> {
        let mut batches: BTreeMap<u32, Vec<SerializedEvent>> = BTreeMap::new();
        for event in events {
            let partition = self.partitioning.partition_for(event)?;
            self.ensure_owned(partition.id())?;
            batches
                .entry(partition.id())
                .or_default()
                .push(SerializedEvent::from_event(event)?);
        }
        Ok(batches)
    }
}

impl Writer for FsWriter {
    async fn write(&self, event: &Event) -> Result<()> {
        let partition = self.partitioning.partition_for(event)?;
        let serialized = SerializedEvent::from_event(event)?;
        self.with_log(partition.id(), move |log| {
            log.append(serialized).map(|_| ())
        })
        .await
    }

    async fn write_all(&self, events: &[Event]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        for (id, serialized) in self.group(events)? {
            self.with_log(id, move |log| log.append_all(serialized).map(|_| ()))
                .await?;
        }
        Ok(())
    }
}
