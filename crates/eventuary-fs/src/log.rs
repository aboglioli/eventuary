use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use eventuary_core::{Result, SerializedEvent};
use fs4::fs_std::FileExt;

use crate::error::{corrupt, io_at, store};
use crate::index::{OffsetIndex, TimeIndex};
use crate::layout::{
    LOG_SUFFIX, OFFSET_INDEX_SUFFIX, TIME_INDEX_SUFFIX, lock_path, parse_segment_base,
    partition_dir, segment_path,
};
use crate::segment::Segment;

pub use crate::record::Record;
pub use crate::segment::SegmentConfig;

pub const DEFAULT_SYNC_INTERVAL_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SyncPolicy {
    Never,
    Always,
    EveryBytes(u64),
}

impl Default for SyncPolicy {
    fn default() -> Self {
        Self::EveryBytes(DEFAULT_SYNC_INTERVAL_BYTES)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RetentionPolicy {
    pub max_bytes: Option<u64>,
    pub max_age: Option<Duration>,
}

impl RetentionPolicy {
    pub fn is_enabled(&self) -> bool {
        self.max_bytes.is_some() || self.max_age.is_some()
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LogConfig {
    pub segment: SegmentConfig,
    pub sync: SyncPolicy,
    pub retention: RetentionPolicy,
}

pub struct PartitionLog {
    dir: PathBuf,
    segments: Vec<Segment>,
    config: LogConfig,
    bytes_since_sync: u64,
    lock: Option<File>,
}

impl PartitionLog {
    pub fn open_readonly(root: &Path, partition_id: u32, config: LogConfig) -> Result<Self> {
        Self::open_inner(root, partition_id, config, false)
    }

    pub fn open_writable(root: &Path, partition_id: u32, config: LogConfig) -> Result<Self> {
        Self::open_inner(root, partition_id, config, true)
    }

    fn open_inner(
        root: &Path,
        partition_id: u32,
        config: LogConfig,
        writable: bool,
    ) -> Result<Self> {
        let dir = partition_dir(root, partition_id);
        fs::create_dir_all(&dir).map_err(|e| io_at("create partition dir", &dir, e))?;

        let lock = if writable {
            let path = lock_path(&dir);
            let file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&path)
                .map_err(|e| io_at("open partition lock", &path, e))?;
            file.try_lock_exclusive()
                .map_err(|e| store(format!("lock partition {partition_id}"), e))
                .and_then(|acquired| {
                    if acquired {
                        Ok(())
                    } else {
                        Err(store(
                            format!("lock partition {partition_id}"),
                            "already held by another writer",
                        ))
                    }
                })?;
            Some(file)
        } else {
            None
        };

        let mut bases = discover_segment_bases(&dir)?;
        if bases.is_empty() {
            bases.push(0);
        }
        let last = bases.len() - 1;
        let mut segments = Vec::with_capacity(bases.len());
        for (i, base) in bases.into_iter().enumerate() {
            segments.push(Segment::open(
                &dir,
                base,
                config.segment,
                writable && i == last,
            )?);
        }

        Ok(Self {
            dir,
            segments,
            config,
            bytes_since_sync: 0,
            lock,
        })
    }

    pub fn start_offset(&self) -> u64 {
        self.segments
            .first()
            .map(|s| s.base_offset())
            .unwrap_or_default()
    }

    pub fn next_offset(&self) -> u64 {
        self.segments
            .last()
            .map(|s| s.next_offset())
            .unwrap_or_default()
    }

    pub fn is_empty(&self) -> bool {
        self.next_offset() == self.start_offset()
    }

    pub fn append(&mut self, event: SerializedEvent) -> Result<u64> {
        let offset = self.next_offset();
        let timestamp_ms = event.timestamp.timestamp_millis();
        let record = Record::new(offset, event);
        let encoded_len = record.encode()?.len() as u64;

        if self.active()?.would_exceed(encoded_len) {
            self.roll(offset)?;
        }
        self.active()?.append(&record, timestamp_ms)?;
        self.bytes_since_sync += encoded_len;
        self.maybe_sync()?;
        Ok(offset)
    }

    pub fn append_all(&mut self, events: Vec<SerializedEvent>) -> Result<Vec<u64>> {
        let mut offsets = Vec::with_capacity(events.len());
        for event in events {
            offsets.push(self.append(event)?);
        }
        self.sync()?;
        Ok(offsets)
    }

    pub fn read(&self, from_offset: u64, max_records: usize) -> Result<Vec<Record>> {
        if max_records == 0 {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        let start = self.segment_index_for(from_offset);
        for segment in &self.segments[start..] {
            if out.len() >= max_records {
                break;
            }
            let from = from_offset.max(segment.base_offset());
            out.extend(segment.read_from(from, max_records - out.len())?);
        }
        Ok(out)
    }

    pub fn offset_for_timestamp(&self, timestamp_ms: i64) -> Option<u64> {
        for segment in &self.segments {
            if let Some(offset) = segment.seek_offset_for_timestamp(timestamp_ms) {
                let found = segment
                    .read_from(offset, 4096)
                    .ok()?
                    .into_iter()
                    .find(|r| r.event.timestamp.timestamp_millis() >= timestamp_ms)
                    .map(|r| r.offset);
                if found.is_some() {
                    return found;
                }
            }
        }
        None
    }

    pub fn refresh(&mut self) -> Result<()> {
        let bases = discover_segment_bases(&self.dir)?;
        if bases.is_empty() {
            return Ok(());
        }
        let known = self.segments.len();
        for (i, base) in bases.iter().enumerate() {
            if i < known && self.segments[i].base_offset() == *base && i + 1 < bases.len() {
                continue;
            }
            let segment = Segment::open(&self.dir, *base, self.config.segment, false)?;
            if i < self.segments.len() {
                self.segments[i] = segment;
            } else {
                self.segments.push(segment);
            }
        }
        self.segments.truncate(bases.len());
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        if let Some(segment) = self.segments.last_mut() {
            segment.sync()?;
        }
        self.bytes_since_sync = 0;
        Ok(())
    }

    pub fn enforce_retention(&mut self) -> Result<usize> {
        if !self.config.retention.is_enabled() || self.segments.len() < 2 {
            return Ok(0);
        }
        let now = SystemTime::now();
        let mut removed = 0usize;
        while self.segments.len() > 1 {
            let expired_by_age = match self.config.retention.max_age {
                Some(max_age) => segment_age(&self.dir, self.segments[0].base_offset())
                    .map(|age| age > max_age)
                    .unwrap_or(false),
                None => false,
            };
            let expired_by_size = match self.config.retention.max_bytes {
                Some(max_bytes) => self.total_bytes() > max_bytes,
                None => false,
            };
            if !expired_by_age && !expired_by_size {
                break;
            }
            let segment = self.segments.remove(0);
            segment.delete()?;
            removed += 1;
        }
        let _ = now;
        Ok(removed)
    }

    pub fn total_bytes(&self) -> u64 {
        self.segments.iter().map(|s| s.size()).sum()
    }

    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    fn active(&mut self) -> Result<&mut Segment> {
        self.segments
            .last_mut()
            .ok_or_else(|| corrupt(Path::new("."), "partition has no active segment"))
    }

    fn roll(&mut self, base_offset: u64) -> Result<()> {
        self.sync()?;
        let segment = Segment::open(&self.dir, base_offset, self.config.segment, true)?;
        self.segments.push(segment);
        Ok(())
    }

    fn maybe_sync(&mut self) -> Result<()> {
        match self.config.sync {
            SyncPolicy::Never => Ok(()),
            SyncPolicy::Always => self.sync(),
            SyncPolicy::EveryBytes(threshold) => {
                if self.bytes_since_sync >= threshold {
                    self.sync()
                } else {
                    Ok(())
                }
            }
        }
    }

    fn segment_index_for(&self, offset: u64) -> usize {
        match self
            .segments
            .binary_search_by_key(&offset, |s| s.base_offset())
        {
            Ok(i) => i,
            Err(0) => 0,
            Err(i) => i - 1,
        }
    }
}

impl Drop for PartitionLog {
    fn drop(&mut self) {
        if let Some(lock) = self.lock.take() {
            let _ = FileExt::unlock(&lock);
        }
    }
}

fn discover_segment_bases(dir: &Path) -> Result<Vec<u64>> {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(io_at("read partition dir", dir, e)),
    };
    let mut bases = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| io_at("read partition dir entry", dir, e))?;
        if let Some(base) = parse_segment_base(&entry.path()) {
            bases.push(base);
        }
    }
    bases.sort_unstable();
    Ok(bases)
}

fn segment_age(dir: &Path, base_offset: u64) -> Option<Duration> {
    let path = segment_path(dir, base_offset, LOG_SUFFIX);
    let modified = fs::metadata(path).ok()?.modified().ok()?;
    SystemTime::now().duration_since(modified).ok()
}

pub fn index_entry_counts(dir: &Path, base_offset: u64) -> Result<(usize, usize)> {
    let offsets = OffsetIndex::open(segment_path(dir, base_offset, OFFSET_INDEX_SUFFIX))?;
    let times = TimeIndex::open(segment_path(dir, base_offset, TIME_INDEX_SUFFIX))?;
    Ok((offsets.entries().len(), times.entries().len()))
}
