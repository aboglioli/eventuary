use std::collections::{BTreeMap, HashMap, VecDeque};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use eventuary_core::io::cursor::{CursorOrder, JsonCursorCodec};
use eventuary_core::io::filter::EventFilter;
use eventuary_core::io::reader::{
    CoordinatedAcker, CoordinatedCursor, CoordinatedReader, CoordinatedReaderConfig,
    CoordinatedStream, CoordinatedSubscription, PartitionAcker, PartitionedCoordAdapter,
    PartitionedCursor,
};
use eventuary_core::io::stream::SpawnedStream;
use eventuary_core::io::{Acker, Cursor, Filter, Message, Reader};
use eventuary_core::partition::{HasPartition, Partition, PartitionGroup, PartitionSelection};
use eventuary_core::{
    Error, PartitionableSubscription, Result, StartFrom, StartableSubscription, StopAt,
};
use tokio::sync::{Mutex, Notify, mpsc};

use crate::coordinator::FsPartitionCoordinator;
use crate::error::{corrupt, join};
use crate::log::{LogConfig, PartitionLog};
use crate::meta::LogMeta;
use crate::record::Record;

#[derive(
    Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct FsCursor {
    pub partition: Partition,
    pub offset: u64,
}

impl FsCursor {
    pub fn new(partition: Partition, offset: u64) -> Self {
        Self { partition, offset }
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn codec() -> Result<JsonCursorCodec<Self>> {
        JsonCursorCodec::new("eventuary.fs.fs_cursor.v1")
    }
}

impl Cursor for FsCursor {
    fn id(&self) -> eventuary_core::io::CursorId {
        eventuary_core::io::CursorId::partition(self.partition)
    }

    fn order_key(&self) -> CursorOrder {
        CursorOrder::from_u64(self.offset)
    }
}

impl HasPartition for FsCursor {
    fn partition(&self) -> Partition {
        self.partition
    }
}

#[derive(Debug, Clone)]
pub struct FsSubscription {
    pub start: StartFrom<FsCursor>,
    pub partition_starts: BTreeMap<u32, u64>,
    pub stop_at: StopAt<FsCursor>,
    pub filter: EventFilter,
    pub partitions: PartitionSelection,
    pub batch_size: Option<usize>,
    pub limit: Option<usize>,
}

impl Default for FsSubscription {
    fn default() -> Self {
        Self {
            start: StartFrom::Latest,
            partition_starts: BTreeMap::new(),
            stop_at: StopAt::Never,
            filter: EventFilter::default(),
            partitions: PartitionSelection::default(),
            batch_size: None,
            limit: None,
        }
    }
}

impl FsSubscription {
    pub fn earliest() -> Self {
        Self {
            start: StartFrom::Earliest,
            ..Self::default()
        }
    }
}

impl StartableSubscription<FsCursor> for FsSubscription {
    fn with_start(mut self, start: StartFrom<FsCursor>) -> Self {
        if let StartFrom::After(cursor) = &start {
            self.partition_starts
                .insert(cursor.partition.id(), cursor.offset + 1);
        }
        self.start = start;
        self
    }

    fn with_starts(mut self, starts: Vec<StartFrom<FsCursor>>) -> Self {
        let mut fallback = None;
        for start in starts {
            match start {
                StartFrom::After(cursor) => {
                    let entry = self
                        .partition_starts
                        .entry(cursor.partition.id())
                        .or_insert(cursor.offset + 1);
                    *entry = (*entry).min(cursor.offset + 1);
                }
                other => fallback = Some(other),
            }
        }
        if let Some(other) = fallback
            && self.partition_starts.is_empty()
        {
            self.start = other;
        }
        self
    }
}

impl PartitionableSubscription<FsCursor> for FsSubscription {
    fn with_partitions(mut self, group: PartitionGroup) -> Self {
        self.partitions = PartitionSelection::Many(group);
        self
    }
}

#[derive(Debug, Clone)]
pub struct FsReaderConfig {
    pub log: LogConfig,
    pub poll_interval: Duration,
    pub default_batch_size: usize,
}

impl Default for FsReaderConfig {
    fn default() -> Self {
        Self {
            log: LogConfig::default(),
            poll_interval: Duration::from_millis(50),
            default_batch_size: 100,
        }
    }
}

#[derive(Clone)]
pub struct FsCursorAcker {
    state: Arc<Mutex<AckState>>,
    notify: Arc<Notify>,
    partition_id: u32,
    offset: u64,
}

struct AckState {
    last_acked: HashMap<u32, u64>,
    pending_nack: bool,
}

impl AckState {
    fn acked(&self, partition_id: u32, offset: u64) -> bool {
        self.last_acked
            .get(&partition_id)
            .is_some_and(|acked| *acked >= offset)
    }
}

impl Acker for FsCursorAcker {
    async fn ack(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        let entry = state.last_acked.entry(self.partition_id).or_default();
        if self.offset >= *entry {
            *entry = self.offset;
        }
        state.pending_nack = false;
        self.notify.notify_waiters();
        Ok(())
    }

    async fn nack(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        state.pending_nack = true;
        self.notify.notify_waiters();
        Ok(())
    }
}

#[derive(Clone)]
pub struct FsReader {
    root: PathBuf,
    partition_count: NonZeroU32,
    config: FsReaderConfig,
}

impl FsReader {
    pub fn open(root: impl Into<PathBuf>, config: FsReaderConfig) -> Result<Self> {
        let root = root.into();
        let meta = LogMeta::load(&root)?
            .ok_or_else(|| Error::Config(format!("no event log found at {}", root.display())))?;
        let partition_count = NonZeroU32::new(meta.partition_count)
            .ok_or_else(|| Error::Config("partition count must be non-zero".to_owned()))?;
        Ok(Self {
            root,
            partition_count,
            config,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn partition_count(&self) -> NonZeroU32 {
        self.partition_count
    }

    fn selected_partitions(&self, selection: &PartitionSelection) -> Result<Vec<Partition>> {
        match selection {
            PartitionSelection::All => (0..self.partition_count.get())
                .map(|id| Partition::new(id, self.partition_count))
                .collect(),
            PartitionSelection::One(partition) => Ok(vec![*partition]),
            PartitionSelection::Many(group) => Ok(group.partitions().to_vec()),
        }
    }
}

struct PartitionCursor {
    partition: Partition,
    log: PartitionLog,
    next: u64,
    stop: Option<u64>,
    exhausted: bool,
}

impl Reader for FsReader {
    type Subscription = FsSubscription;
    type Acker = FsCursorAcker;
    type Cursor = FsCursor;
    type Stream = SpawnedStream<FsCursorAcker, FsCursor>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let (tx, rx) = mpsc::channel(64);
        let partitions = match self.selected_partitions(&subscription.partitions) {
            Ok(p) => p,
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                return Ok(SpawnedStream::from_receiver(rx));
            }
        };

        let root = self.root.clone();
        let log_config = self.config.log;
        let sub = subscription.clone();
        let opened =
            tokio::task::spawn_blocking(move || open_cursors(&root, &partitions, &sub, log_config))
                .await
                .map_err(join)?;
        let mut cursors = match opened {
            Ok(c) => c,
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                return Ok(SpawnedStream::from_receiver(rx));
            }
        };

        let batch_size = subscription
            .batch_size
            .unwrap_or(self.config.default_batch_size)
            .clamp(1, 10_000);
        let poll_interval = self.config.poll_interval;
        let filter = subscription.filter.clone();
        let limit = subscription.limit;
        let bounded = !matches!(subscription.stop_at, StopAt::Never);

        let state = Arc::new(Mutex::new(AckState {
            last_acked: HashMap::new(),
            pending_nack: false,
        }));
        let notify = Arc::new(Notify::new());

        let handle = tokio::spawn(async move {
            let mut delivered = 0usize;
            let mut buffer: VecDeque<(Record, Partition)> = VecDeque::new();
            loop {
                if buffer.is_empty() {
                    let taken = std::mem::take(&mut cursors);
                    let joined = tokio::task::spawn_blocking(move || {
                        let mut taken = taken;
                        let fetched = fetch_round(&mut taken, batch_size);
                        (taken, fetched)
                    })
                    .await;
                    let fetched = match joined {
                        Ok((returned, fetched)) => {
                            cursors = returned;
                            match fetched {
                                Ok(f) => f,
                                Err(e) => {
                                    let _ = tx.send(Err(e)).await;
                                    return;
                                }
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(Err(join(e))).await;
                            return;
                        }
                    };
                    if fetched.is_empty() {
                        if bounded && cursors.iter().all(|c| c.exhausted) {
                            return;
                        }
                        tokio::time::sleep(poll_interval).await;
                        continue;
                    }
                    buffer.extend(fetched);
                }

                while let Some((record, partition)) = buffer.front() {
                    let partition = *partition;
                    let offset = record.offset;
                    let event = match record.event.to_event() {
                        Ok(e) => e,
                        Err(e) => {
                            let _ = tx
                                .send(Err(Error::Serialization(format!(
                                    "decode event at partition {} offset {offset}: {e}",
                                    partition.id()
                                ))))
                                .await;
                            return;
                        }
                    };
                    if !filter.matches(&event) {
                        buffer.pop_front();
                        advance(&mut cursors, partition.id(), offset);
                        continue;
                    }
                    if let Some(l) = limit
                        && delivered >= l
                    {
                        return;
                    }
                    let acker = FsCursorAcker {
                        state: Arc::clone(&state),
                        notify: Arc::clone(&notify),
                        partition_id: partition.id(),
                        offset,
                    };
                    let cursor = FsCursor::new(partition, offset);
                    if tx
                        .send(Ok(Message::new(event, acker, cursor)))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    delivered += 1;

                    let mut nacked = false;
                    loop {
                        let settled = notify.notified();
                        tokio::pin!(settled);
                        settled.as_mut().enable();
                        {
                            let mut guard = state.lock().await;
                            if guard.acked(partition.id(), offset) {
                                buffer.pop_front();
                                advance(&mut cursors, partition.id(), offset);
                                break;
                            }
                            if guard.pending_nack {
                                guard.pending_nack = false;
                                nacked = true;
                                buffer.clear();
                                break;
                            }
                            if tx.is_closed() {
                                return;
                            }
                        }
                        settled.await;
                    }
                    if nacked {
                        tokio::time::sleep(poll_interval).await;
                    }
                    if buffer.is_empty() {
                        break;
                    }
                }
            }
        });

        Ok(SpawnedStream::new(rx, handle))
    }
}

fn advance(cursors: &mut [PartitionCursor], partition_id: u32, offset: u64) {
    if let Some(cursor) = cursors
        .iter_mut()
        .find(|c| c.partition.id() == partition_id)
    {
        cursor.next = cursor.next.max(offset + 1);
    }
}

/// Offsets are dense within a partition, so a gap means either a stale view — worth a
/// refresh — or, if it survives one, an event the log lost.
fn first_gap(records: &[Record], next: u64) -> Option<u64> {
    (next..)
        .zip(records)
        .find(|(expected, record)| record.offset != *expected)
        .map(|(expected, _)| expected)
}

fn missing_offset(cursor: &PartitionCursor, offset: u64) -> Error {
    corrupt(
        cursor.log.dir(),
        format!(
            "partition {} has no event at offset {offset}",
            cursor.partition.id()
        ),
    )
}

fn retention_gap(cursor: &PartitionCursor) -> Result<()> {
    let start = cursor.log.start_offset();
    if cursor.next >= start {
        return Ok(());
    }
    Err(Error::InvalidCursor(format!(
        "partition {} resumes at offset {} but its log now starts at {start}; \
         retention removed {} unread event(s)",
        cursor.partition.id(),
        cursor.next,
        start - cursor.next
    )))
}

fn fetch_round(
    cursors: &mut [PartitionCursor],
    batch_size: usize,
) -> Result<Vec<(Record, Partition)>> {
    let mut out = Vec::new();
    for cursor in cursors.iter_mut() {
        if cursor.exhausted {
            continue;
        }
        retention_gap(cursor)?;
        let take = match cursor.stop {
            Some(stop) if cursor.next > stop => {
                cursor.exhausted = true;
                continue;
            }
            Some(stop) => batch_size.min((stop - cursor.next + 1) as usize),
            None => batch_size,
        };
        let mut records = cursor.log.read(cursor.next, take)?;
        if records.is_empty() || first_gap(&records, cursor.next).is_some() {
            cursor.log.refresh()?;
            retention_gap(cursor)?;
            records = cursor.log.read(cursor.next, take)?;
            if let Some(missing) = first_gap(&records, cursor.next) {
                return Err(missing_offset(cursor, missing));
            }
        }
        if records.is_empty() {
            if cursor.stop.is_some() && cursor.next >= cursor.log.next_offset() {
                cursor.exhausted = true;
            }
            continue;
        }
        for record in records {
            out.push((record, cursor.partition));
        }
    }
    Ok(out)
}

fn open_cursors(
    root: &Path,
    partitions: &[Partition],
    subscription: &FsSubscription,
    config: LogConfig,
) -> Result<Vec<PartitionCursor>> {
    let mut cursors = Vec::with_capacity(partitions.len());
    for partition in partitions {
        let log = PartitionLog::open_readonly(root, partition.id(), config)?;
        let next = resolve_start(&log, partition, subscription)?;
        let stop = resolve_stop(&log, partition, subscription);
        cursors.push(PartitionCursor {
            partition: *partition,
            log,
            next,
            stop,
            exhausted: false,
        });
    }
    Ok(cursors)
}

fn resolve_start(
    log: &PartitionLog,
    partition: &Partition,
    subscription: &FsSubscription,
) -> Result<u64> {
    if let Some(offset) = subscription.partition_starts.get(&partition.id()) {
        return Ok(*offset);
    }
    Ok(match &subscription.start {
        StartFrom::Earliest => log.start_offset(),
        StartFrom::Latest => log.next_offset(),
        StartFrom::After(cursor) => {
            if cursor.partition.id() == partition.id() {
                cursor.offset + 1
            } else {
                log.next_offset()
            }
        }
        StartFrom::Timestamp(ts) => log
            .offset_for_timestamp(ts.timestamp_millis())
            .unwrap_or_else(|| log.next_offset()),
    })
}

fn resolve_stop(
    log: &PartitionLog,
    partition: &Partition,
    subscription: &FsSubscription,
) -> Option<u64> {
    match &subscription.stop_at {
        StopAt::Never => None,
        StopAt::CurrentEnd => Some(log.next_offset().saturating_sub(1)),
        StopAt::Cursor(cursor) => {
            if cursor.partition.id() == partition.id() {
                Some(cursor.offset)
            } else {
                Some(log.next_offset().saturating_sub(1))
            }
        }
    }
}

/// Composed aliases pairing this backend's reader with its partition
/// coordinator, mirroring the shape every other backend exposes so the module
/// path is learned once and reused.
pub type FsPartitionedCursor = PartitionedCursor<FsCursor>;
pub type FsCoordinatedReaderConfig = CoordinatedReaderConfig;
pub type FsCoordinatedSubscription = CoordinatedSubscription<FsSubscription, FsCursor>;
pub type FsCoordinatedReader = CoordinatedReader<FsReader, FsPartitionCoordinator<FsCursor>>;
pub type FsCoordinatedAcker =
    CoordinatedAcker<FsCursorAcker, FsCursor, FsPartitionCoordinator<FsCursor>>;
pub type FsCoordinatedStreamAcker = CoordinatedAcker<
    PartitionAcker<FsCursorAcker, FsCursor>,
    PartitionedCursor<FsCursor>,
    PartitionedCoordAdapter<FsPartitionCoordinator<FsCursor>, FsCursor>,
>;
pub type FsCoordinatedCursor = CoordinatedCursor<PartitionedCursor<FsCursor>>;
pub type FsCoordinatedStream = CoordinatedStream<
    PartitionAcker<FsCursorAcker, FsCursor>,
    PartitionedCursor<FsCursor>,
    PartitionedCoordAdapter<FsPartitionCoordinator<FsCursor>, FsCursor>,
>;
