//! Generic coordinated reader: distributes partition ownership across instances.
//!
//! `CoordinatedReader<R, Coord>` wraps an inner partition-aware `Reader` with
//! a `PartitionCoordinator<C>` to provide multi-instance partition leasing,
//! dynamic rebalance, lease renewal, monotonic checkpointing, and graceful
//! release on shutdown. Backend crates (`eventuary-postgres`,
//! `eventuary-sqlite`) ship as thin `pub type` aliases over this generic.
//!
//! The `PartitionCoordinator<C>` trait, `PartitionLease<C>`, and `Generation`
//! fencing token live in this module too — they are the protocol surface
//! every backend must implement to plug into `CoordinatedReader`. Colocating
//! the trait with its sole consumer follows the same shape as
//! `CheckpointStore` in `checkpoint.rs`, `BufferStore` in `buffer.rs`, etc.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::num::{NonZeroU32, NonZeroUsize};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::Stream;
use futures::StreamExt;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Notify, mpsc};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::Result;
use crate::error::Error;
use crate::io::cursor::CursorOrder;
use crate::io::position::{PartitionableSubscription, StartFrom, StartableSubscription};
use crate::io::reader::CheckpointScope;
use crate::io::reader::partitioned::{
    PartitionAcker, PartitionedCursor, PartitionedReader, PartitionedReaderConfig,
    PartitionedSubscription,
};
use crate::io::{Acker, Cursor, CursorId, Message, OwnerId, Reader};
use crate::partition::{HasPartition, Partition, PartitionGroup};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Generation(i64);

impl Generation {
    pub fn initial() -> Self {
        Self(0)
    }

    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }

    pub fn get(self) -> i64 {
        self.0
    }

    pub fn from_i64(value: i64) -> Self {
        Self(value)
    }
}

impl fmt::Display for Generation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A lease on a single partition granted by a [`PartitionCoordinator`].
///
/// `checkpoint_cursor` encodes the last durable progress position for this
/// partition. All backends use `None` to signal "no prior progress":
/// - Memory: `None` for a freshly-claimed partition, `Some(C)` after the
///   first `checkpoint` call.
/// - Postgres and SQLite: `None` when the stored `checkpoint_sequence` is
///   the sentinel `0` (no checkpoint has ever been committed; SQL event
///   sequences start at `1`), and `Some(C)` once a checkpoint advances it.
///   `CoordinatedReader` falls back to the subscription's `start` whenever
///   the cursor is `None`.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PartitionLease<C> {
    pub scope: CheckpointScope,
    pub owner_id: OwnerId,
    pub partition: Partition,
    pub generation: Generation,
    pub checkpoint_cursor: Option<C>,
    pub lease_until: DateTime<Utc>,
}

pub trait PartitionCoordinator<C>: Clone + Send + Sync + 'static
where
    C: Cursor + Clone + Send + Sync + 'static,
{
    fn heartbeat<'a>(
        &'a self,
        scope: &'a CheckpointScope,
        owner_id: &'a OwnerId,
        lease_duration: Duration,
    ) -> impl Future<Output = Result<()>> + Send + 'a;

    fn live_consumers<'a>(
        &'a self,
        scope: &'a CheckpointScope,
    ) -> impl Future<Output = Result<usize>> + Send + 'a;

    /// Remove this consumer's registration so other consumers stop counting it
    /// as live when computing `target_partition_count`. Called by
    /// `CoordinatedReader` on stream shutdown. Should be idempotent and must
    /// not return an error when the consumer record is missing.
    fn release_consumer<'a>(
        &'a self,
        scope: &'a CheckpointScope,
        owner_id: &'a OwnerId,
    ) -> impl Future<Output = Result<()>> + Send + 'a;

    /// Attempt to take a partition. Returns `Ok(Some(lease))` on success or
    /// `Ok(None)` if another live owner holds it. Increments `generation` on
    /// every successful claim.
    fn claim<'a>(
        &'a self,
        scope: &'a CheckpointScope,
        owner_id: &'a OwnerId,
        partition: Partition,
        lease_duration: Duration,
    ) -> impl Future<Output = Result<Option<PartitionLease<C>>>> + Send + 'a;

    /// Extend `lease_until` only if `(owner_id, generation)` still matches.
    /// Returns `Err(Error::OwnershipLost(_))` on mismatch.
    fn renew<'a>(
        &'a self,
        lease: &'a PartitionLease<C>,
        lease_duration: Duration,
    ) -> impl Future<Output = Result<()>> + Send + 'a;

    /// Clear `owner_id` and `lease_until` only if `(owner_id, generation)`
    /// matches. Increments `generation`. Returns `Err(Error::OwnershipLost(_))`
    /// on mismatch.
    fn release<'a>(
        &'a self,
        lease: &'a PartitionLease<C>,
    ) -> impl Future<Output = Result<()>> + Send + 'a;

    /// Write `cursor` only if `(owner_id, generation)` matches. The update
    /// is monotonic via `Cursor::order_key()`. Returns
    /// `Err(Error::OwnershipLost(_))` on mismatch.
    fn checkpoint<'a>(
        &'a self,
        lease: &'a PartitionLease<C>,
        cursor: C,
    ) -> impl Future<Output = Result<()>> + Send + 'a;
}

/// Adapter that lifts a `PartitionCoordinator<C>` to
/// `PartitionCoordinator<PartitionedCursor<C>>`.
///
/// The shared-fetch data plane in `CoordinatedReader::read` composes
/// `PartitionedReader::source_from_cursor` over the inner partition-aware
/// reader, which turns every emitted cursor into
/// `PartitionedCursor<R::Cursor>`. The acker the coordinator fences against
/// therefore carries `PartitionedCursor<R::Cursor>`, not the raw `R::Cursor`.
///
/// Backends only implement `PartitionCoordinator<R::Cursor>` (e.g.
/// `PartitionCoordinator<PgCursor>`). This adapter wraps and unwraps the
/// `PartitionedCursor` envelope so no backend needs to know about the lane
/// scheduler. The wrap reuses `lease.partition` for the partition field,
/// which is already known on every codepath and matches the writer's stamped
/// partition.
///
/// A direct blanket `impl<T, C> PartitionCoordinator<PartitionedCursor<C>> for T
/// where T: PartitionCoordinator<C>` would type-recurse on itself
/// (`PartitionedCursor<PartitionedCursor<C>>` …) and overflow rustc. An
/// explicit adapter type breaks the recursion.
pub struct PartitionedCoordAdapter<Coord, C> {
    inner: Arc<Coord>,
    _cursor: std::marker::PhantomData<fn() -> C>,
}

impl<Coord, C> PartitionedCoordAdapter<Coord, C> {
    pub fn new(inner: Arc<Coord>) -> Self {
        Self {
            inner,
            _cursor: std::marker::PhantomData,
        }
    }
}

impl<Coord, C> Clone for PartitionedCoordAdapter<Coord, C> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            _cursor: std::marker::PhantomData,
        }
    }
}

impl<Coord, C> PartitionCoordinator<PartitionedCursor<C>> for PartitionedCoordAdapter<Coord, C>
where
    Coord: PartitionCoordinator<C>,
    C: Cursor + Clone + Ord + Send + Sync + 'static,
{
    async fn heartbeat<'a>(
        &'a self,
        scope: &'a CheckpointScope,
        owner_id: &'a OwnerId,
        lease_duration: Duration,
    ) -> Result<()> {
        self.inner.heartbeat(scope, owner_id, lease_duration).await
    }

    async fn live_consumers<'a>(&'a self, scope: &'a CheckpointScope) -> Result<usize> {
        self.inner.live_consumers(scope).await
    }

    async fn release_consumer<'a>(
        &'a self,
        scope: &'a CheckpointScope,
        owner_id: &'a OwnerId,
    ) -> Result<()> {
        self.inner.release_consumer(scope, owner_id).await
    }

    async fn claim<'a>(
        &'a self,
        scope: &'a CheckpointScope,
        owner_id: &'a OwnerId,
        partition: Partition,
        lease_duration: Duration,
    ) -> Result<Option<PartitionLease<PartitionedCursor<C>>>> {
        let inner_lease = self
            .inner
            .claim(scope, owner_id, partition, lease_duration)
            .await?;
        Ok(inner_lease.map(|l| wrap_lease(l, partition)))
    }

    async fn renew<'a>(
        &'a self,
        lease: &'a PartitionLease<PartitionedCursor<C>>,
        lease_duration: Duration,
    ) -> Result<()> {
        let inner_lease = unwrap_lease_ref(lease);
        self.inner.renew(&inner_lease, lease_duration).await
    }

    async fn release<'a>(&'a self, lease: &'a PartitionLease<PartitionedCursor<C>>) -> Result<()> {
        let inner_lease = unwrap_lease_ref(lease);
        self.inner.release(&inner_lease).await
    }

    async fn checkpoint<'a>(
        &'a self,
        lease: &'a PartitionLease<PartitionedCursor<C>>,
        cursor: PartitionedCursor<C>,
    ) -> Result<()> {
        let inner_lease = unwrap_lease_ref(lease);
        self.inner
            .checkpoint(&inner_lease, cursor.into_inner())
            .await
    }
}

fn wrap_lease<C>(
    lease: PartitionLease<C>,
    partition: Partition,
) -> PartitionLease<PartitionedCursor<C>> {
    PartitionLease {
        scope: lease.scope,
        owner_id: lease.owner_id,
        partition: lease.partition,
        generation: lease.generation,
        checkpoint_cursor: lease
            .checkpoint_cursor
            .map(|c| PartitionedCursor::new(c, partition)),
        lease_until: lease.lease_until,
    }
}

fn unwrap_lease_ref<C: Clone>(lease: &PartitionLease<PartitionedCursor<C>>) -> PartitionLease<C> {
    PartitionLease {
        scope: lease.scope.clone(),
        owner_id: lease.owner_id.clone(),
        partition: lease.partition,
        generation: lease.generation,
        checkpoint_cursor: lease.checkpoint_cursor.as_ref().map(|c| c.inner().clone()),
        lease_until: lease.lease_until,
    }
}

/// Buffering policy for per-partition checkpoint flushes.
///
/// `CoordinatedReader` collapses repeated `coordinator.checkpoint(...)` calls
/// per partition into a single flush, controlled by either a count or a time
/// threshold. The default policy (`max_pending_acks = 1`,
/// `max_pending_interval = ZERO`) flushes on every ack, matching the
/// historical behavior.
///
/// Events acked between flushes will be redelivered on owner crash. Handlers
/// must already be idempotent for redelivery.
#[derive(Debug, Clone, Copy)]
pub struct CheckpointFlushPolicy {
    pub max_pending_acks: NonZeroUsize,
    pub max_pending_interval: Duration,
}

impl Default for CheckpointFlushPolicy {
    fn default() -> Self {
        Self {
            max_pending_acks: NonZeroUsize::new(1).unwrap(),
            max_pending_interval: Duration::ZERO,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CoordinatedReaderConfig {
    pub partition_lease_duration: Duration,
    pub partition_renew_interval: Duration,
    pub consumer_lease_duration: Duration,
    pub consumer_heartbeat_interval: Duration,
    pub rebalance_interval: Duration,
    pub partition_slack: u32,
    pub checkpoint_flush: CheckpointFlushPolicy,
    /// Per-lane buffer capacity wired through to the internal
    /// `PartitionedReaderConfig`. Defaults match
    /// `PartitionedReaderConfig::default().lane_capacity`.
    pub lane_capacity: NonZeroUsize,
}

impl Default for CoordinatedReaderConfig {
    fn default() -> Self {
        Self {
            partition_lease_duration: Duration::from_secs(60),
            partition_renew_interval: Duration::from_secs(15),
            consumer_lease_duration: Duration::from_secs(30),
            consumer_heartbeat_interval: Duration::from_secs(10),
            rebalance_interval: Duration::from_secs(10),
            partition_slack: 0,
            checkpoint_flush: CheckpointFlushPolicy::default(),
            lane_capacity: NonZeroUsize::new(128).unwrap(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CoordinatedSubscription<S, C> {
    pub inner: S,
    pub scope: CheckpointScope,
    pub partition_count: NonZeroU32,
    pub start: StartFrom<C>,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, serde::Serialize, serde::Deserialize)]
pub struct CoordinatedCursor<C> {
    pub partition: Partition,
    pub inner: C,
}

impl<C> CoordinatedCursor<C> {
    pub fn new(partition: Partition, inner: C) -> Self {
        Self { partition, inner }
    }
}

impl<C: Cursor> Cursor for CoordinatedCursor<C> {
    fn id(&self) -> CursorId {
        CursorId::partition(self.partition)
    }

    fn order_key(&self) -> CursorOrder {
        self.inner.order_key()
    }
}

/// Per-partition checkpoint buffer entry.
///
/// Tracks the highest acked cursor (by `order_key`), the count of
/// un-flushed acks, and the timestamp of the first un-flushed ack since
/// the last flush.
#[derive(Debug)]
pub(crate) struct PendingCheckpoint<C> {
    cursor: C,
    count: usize,
    first_pending_at: Instant,
}

pub(crate) type PendingCheckpoints<C> = Arc<Mutex<HashMap<u32, PendingCheckpoint<C>>>>;

pub struct CoordinatedAcker<A, C, Coord> {
    inner: A,
    coordinator: Arc<Coord>,
    lease: PartitionLease<C>,
    cursor: C,
    pending: PendingCheckpoints<C>,
    policy: CheckpointFlushPolicy,
}

impl<A, C, Coord> CoordinatedAcker<A, C, Coord> {
    /// Construct an acker that flushes the checkpoint on every ack.
    ///
    /// Equivalent to the default `CheckpointFlushPolicy`
    /// (`max_pending_acks = 1`, `max_pending_interval = 0`).
    pub fn new(inner: A, coordinator: Arc<Coord>, lease: PartitionLease<C>, cursor: C) -> Self {
        Self {
            inner,
            coordinator,
            lease,
            cursor,
            pending: Arc::new(Mutex::new(HashMap::new())),
            policy: CheckpointFlushPolicy::default(),
        }
    }

    pub(crate) fn with_buffer(
        inner: A,
        coordinator: Arc<Coord>,
        lease: PartitionLease<C>,
        cursor: C,
        pending: PendingCheckpoints<C>,
        policy: CheckpointFlushPolicy,
    ) -> Self {
        Self {
            inner,
            coordinator,
            lease,
            cursor,
            pending,
            policy,
        }
    }
}

impl<A, C, Coord> Acker for CoordinatedAcker<A, C, Coord>
where
    A: Acker + Send + Sync + 'static,
    C: Cursor + Clone + Send + Sync + 'static,
    Coord: PartitionCoordinator<C>,
{
    async fn ack(&self) -> Result<()> {
        self.inner.ack().await?;

        let partition_id = self.lease.partition.id();
        let to_flush = {
            let mut pending = self.pending.lock().await;
            let now = Instant::now();
            let entry = pending
                .entry(partition_id)
                .or_insert_with(|| PendingCheckpoint {
                    cursor: self.cursor.clone(),
                    count: 0,
                    first_pending_at: now,
                });

            if self.cursor.order_key() > entry.cursor.order_key() {
                entry.cursor = self.cursor.clone();
            }
            entry.count += 1;

            let count_hit = entry.count >= self.policy.max_pending_acks.get();
            let interval_hit = !self.policy.max_pending_interval.is_zero()
                && now.duration_since(entry.first_pending_at) >= self.policy.max_pending_interval;

            if count_hit || interval_hit {
                pending.remove(&partition_id).map(|e| e.cursor)
            } else {
                None
            }
        };

        if let Some(cursor) = to_flush {
            self.coordinator.checkpoint(&self.lease, cursor).await?;
        }
        Ok(())
    }

    async fn nack(&self) -> Result<()> {
        self.inner.nack().await
    }
}

/// Flush the pending checkpoint for `partition_id` (if any) under `lease`.
/// Errors are logged but do not propagate — callers (release/shutdown paths)
/// must continue regardless.
async fn flush_pending_for<C, Coord>(
    pending: &PendingCheckpoints<C>,
    coordinator: &Arc<Coord>,
    lease: &PartitionLease<C>,
    owner_id: &OwnerId,
) where
    C: Cursor + Clone + Send + Sync + 'static,
    Coord: PartitionCoordinator<C>,
{
    let partition_id = lease.partition.id();
    let cursor = {
        let mut pending = pending.lock().await;
        pending.remove(&partition_id).map(|e| e.cursor)
    };
    if let Some(cursor) = cursor
        && let Err(e) = coordinator.checkpoint(lease, cursor).await
    {
        tracing::warn!(
            owner = %owner_id,
            partition = partition_id,
            "coordinated reader: pending checkpoint flush failed: {e}"
        );
    }
}

/// Tick the per-partition interval-based flush. Called from the coordinator
/// loop; for each partition whose oldest pending ack has exceeded
/// `max_pending_interval`, drain and checkpoint.
async fn tick_interval_flush<C, Coord>(
    pending: &PendingCheckpoints<C>,
    coordinator: &Arc<Coord>,
    owned: &HashMap<u32, PartitionLease<C>>,
    owner_id: &OwnerId,
    interval: Duration,
) where
    C: Cursor + Clone + Send + Sync + 'static,
    Coord: PartitionCoordinator<C>,
{
    if interval.is_zero() {
        return;
    }
    let now = Instant::now();
    let due: Vec<(u32, C)> = {
        let mut pending = pending.lock().await;
        let due_ids: Vec<u32> = pending
            .iter()
            .filter(|(_, entry)| now.duration_since(entry.first_pending_at) >= interval)
            .map(|(id, _)| *id)
            .collect();
        due_ids
            .into_iter()
            .filter_map(|id| pending.remove(&id).map(|e| (id, e.cursor)))
            .collect()
    };
    for (partition_id, cursor) in due {
        let Some(lease) = owned.get(&partition_id) else {
            continue;
        };
        if let Err(e) = coordinator.checkpoint(lease, cursor).await {
            tracing::warn!(
                owner = %owner_id,
                partition = partition_id,
                "coordinated reader: interval checkpoint flush failed: {e}"
            );
        }
    }
}

pub struct CoordinatedStream<A, C, Coord, P = crate::Payload>
where
    A: Acker + Send + Sync + 'static,
    C: Cursor + Clone + Send + Sync + 'static,
    Coord: PartitionCoordinator<C>,
{
    #[allow(clippy::type_complexity)]
    rx: mpsc::Receiver<Result<Message<CoordinatedAcker<A, C, Coord>, CoordinatedCursor<C>, P>>>,
    shutdown: Arc<Notify>,
}

impl<A, C, Coord, P> Stream for CoordinatedStream<A, C, Coord, P>
where
    A: Acker + Send + Sync + 'static,
    C: Cursor + Clone + Send + Sync + 'static,
    Coord: PartitionCoordinator<C>,
{
    type Item = Result<Message<CoordinatedAcker<A, C, Coord>, CoordinatedCursor<C>, P>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

impl<A, C, Coord, P> Drop for CoordinatedStream<A, C, Coord, P>
where
    A: Acker + Send + Sync + 'static,
    C: Cursor + Clone + Send + Sync + 'static,
    Coord: PartitionCoordinator<C>,
{
    fn drop(&mut self) {
        self.shutdown.notify_one();
    }
}

pub struct CoordinatedReader<R, Coord> {
    inner: R,
    coordinator: Arc<Coord>,
    owner_id: OwnerId,
    config: CoordinatedReaderConfig,
}

impl<R: Clone, Coord> Clone for CoordinatedReader<R, Coord> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            coordinator: Arc::clone(&self.coordinator),
            owner_id: self.owner_id.clone(),
            config: self.config,
        }
    }
}

impl<R, Coord> CoordinatedReader<R, Coord> {
    pub fn new(
        inner: R,
        coordinator: Arc<Coord>,
        owner_id: OwnerId,
        config: CoordinatedReaderConfig,
    ) -> Self {
        Self {
            inner,
            coordinator,
            owner_id,
            config,
        }
    }
}

fn jittered_duration(base: Duration) -> Duration {
    let jitter_ms = (base.as_millis() as f64 * 0.2) as u64;
    if jitter_ms == 0 {
        return base;
    }
    let extra = rand::rng().random_range(0..=jitter_ms);
    base + Duration::from_millis(extra)
}

fn target_partition_count(total: NonZeroU32, live: usize, slack: u32) -> u32 {
    let total = total.get();
    let live = live.max(1) as u64;
    let base = (total as u64).div_ceil(live) as u32;
    base.saturating_add(slack).min(total)
}

async fn release_owned_partitions_concurrent<C, Coord>(
    owned: &mut HashMap<u32, PartitionLease<C>>,
    coordinator: &Arc<Coord>,
    pending: &PendingCheckpoints<C>,
    owner_id: &OwnerId,
    reason: &'static str,
) where
    C: Cursor + Clone + Send + Sync + 'static,
    Coord: PartitionCoordinator<C>,
{
    let releases = owned.drain().map(|(partition_id, lease)| {
        let coord = Arc::clone(coordinator);
        let pending = Arc::clone(pending);
        let owner_id = owner_id.clone();
        async move {
            flush_pending_for(&pending, &coord, &lease, &owner_id).await;
            let result = coord.release(&lease).await;
            (partition_id, result)
        }
    });
    for (partition_id, result) in futures::future::join_all(releases).await {
        if let Err(e) = result {
            tracing::warn!(
                owner = %owner_id,
                partition = partition_id,
                "coordinated reader: release {reason} failed: {e}"
            );
        }
    }
}

/// Stop the pump (if any) by cancelling its token and awaiting the handle.
/// Returns after the pump task has terminated.
async fn stop_pump(pump: Option<(tokio::task::JoinHandle<()>, CancellationToken)>) {
    if let Some((handle, cancel)) = pump {
        cancel.cancel();
        let _ = handle.await;
    }
}

#[allow(clippy::too_many_arguments)]
fn build_inner_starts<C: Cursor + Clone>(
    owned: &HashMap<u32, PartitionLease<PartitionedCursor<C>>>,
) -> Vec<StartFrom<PartitionedCursor<C>>> {
    owned
        .values()
        .filter_map(|lease| {
            lease
                .checkpoint_cursor
                .as_ref()
                .map(|c| StartFrom::After(c.clone()))
        })
        .collect()
}

impl<R, S, C, A, Coord, P> Reader<P> for CoordinatedReader<R, Coord>
where
    R: Reader<P, Subscription = S, Cursor = C, Acker = A> + Clone + Send + Sync + 'static,
    S: PartitionableSubscription<C> + Clone + Send + Sync + 'static,
    C: Cursor + HasPartition + Clone + Ord + Send + Sync + 'static,
    A: Acker + Clone + Send + Sync + 'static,
    R::Stream: Send + 'static,
    Coord: PartitionCoordinator<C>,
    P: Clone + Send + Sync + 'static,
{
    type Subscription = CoordinatedSubscription<S, C>;
    type Acker = CoordinatedAcker<
        PartitionAcker<A, C, P>,
        PartitionedCursor<C>,
        PartitionedCoordAdapter<Coord, C>,
    >;
    type Cursor = CoordinatedCursor<PartitionedCursor<C>>;
    type Stream = CoordinatedStream<
        PartitionAcker<A, C, P>,
        PartitionedCursor<C>,
        PartitionedCoordAdapter<Coord, C>,
        P,
    >;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        // Wrap the user-supplied coordinator in the adapter so all interior
        // calls operate on `PartitionedCursor<C>`. The adapter is cheap to
        // clone (`Arc` underneath) and breaks the type-level recursion that
        // would arise from a blanket `PartitionCoordinator<PartitionedCursor<C>>`
        // impl on every `PartitionCoordinator<C>`.
        let coordinator = Arc::new(PartitionedCoordAdapter::<Coord, C>::new(Arc::clone(
            &self.coordinator,
        )));

        coordinator
            .heartbeat(
                &subscription.scope,
                &self.owner_id,
                self.config.consumer_lease_duration,
            )
            .await?;

        let (tx, rx) = mpsc::channel::<Result<Message<Self::Acker, Self::Cursor, P>>>(16);
        let shutdown = Arc::new(Notify::new());

        let inner_reader = self.inner.clone();
        let owner_id = self.owner_id.clone();
        let partition_count = subscription.partition_count;
        let scope = subscription.scope.clone();
        let base_sub = subscription.inner.clone();
        let start = subscription.start.clone();
        let partition_lease_duration = self.config.partition_lease_duration;
        let consumer_lease_duration = self.config.consumer_lease_duration;
        let renew_interval = self.config.partition_renew_interval;
        let heartbeat_interval = self.config.consumer_heartbeat_interval;
        let rebalance_interval = self.config.rebalance_interval;
        let slack = self.config.partition_slack;
        let flush_policy = self.config.checkpoint_flush;
        let lane_capacity = self.config.lane_capacity;
        let shutdown_notify = Arc::clone(&shutdown);
        let pending_checkpoints: PendingCheckpoints<PartitionedCursor<C>> =
            Arc::new(Mutex::new(HashMap::new()));
        // Per-partition high-water mark of cursors the pump has forwarded
        // downstream. Survives pump rebuilds (the pump task is reset on every
        // rebalance change) so the new pump can filter out events its
        // predecessor already delivered. Without this, every rebuild reads
        // the inner stream from `Earliest` and re-forwards already-delivered
        // events.
        let forward_high_water: Arc<Mutex<HashMap<u32, PartitionedCursor<C>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        tokio::spawn(async move {
            let mut owned: HashMap<u32, PartitionLease<PartitionedCursor<C>>> = HashMap::new();
            let mut pump: Option<(tokio::task::JoinHandle<()>, CancellationToken)> = None;
            let mut needs_rebuild = false;

            let mut tick_duration = renew_interval.min(heartbeat_interval);
            if !flush_policy.max_pending_interval.is_zero() {
                tick_duration = tick_duration.min(flush_policy.max_pending_interval);
            }

            let mut next_rebalance = tokio::time::Instant::now();
            let mut next_renew = tokio::time::Instant::now() + jittered_duration(tick_duration);

            loop {
                tokio::select! {
                    _ = tokio::time::sleep_until(next_rebalance) => {
                        // Detect pump death: if the pump handle has finished
                        // (inner stream ended, fatal error, etc.) we have to
                        // tear down the lease(s) cleanly and let the next
                        // rebalance pass re-claim if appropriate.
                        //
                        // In practice this is rare: `PgReader` / `SqliteReader`
                        // streams are continuous polling loops and don't end
                        // while the connection is alive. Non-deliberate exits
                        // we expect:
                        //  - inner stream produced `Err(_)`: the error was
                        //    forwarded to the downstream `tx` and the task
                        //    returned. Re-fetching would only re-surface the
                        //    same error; the downstream should drop the
                        //    reader, which fires the shutdown branch.
                        //  - downstream `tx.send` failed: caller dropped the
                        //    stream, the reader is being torn down anyway.
                        // Rebuilding on the next rebalance tick is therefore
                        // intentional — there is no flow to interrupt.
                        let pump_finished = pump
                            .as_ref()
                            .map(|(h, _)| h.is_finished())
                            .unwrap_or(false);
                        if pump_finished {
                            stop_pump(pump.take()).await;
                            let released_ids: Vec<u32> = owned.keys().copied().collect();
                            release_owned_partitions_concurrent(
                                &mut owned,
                                &coordinator,
                                &pending_checkpoints,
                                &owner_id,
                                "on pump exit",
                            ).await;
                            {
                                let mut hw = forward_high_water.lock().await;
                                for id in released_ids {
                                    hw.remove(&id);
                                }
                            }
                            needs_rebuild = false;
                        }

                        let live = match coordinator.live_consumers(&scope).await {
                            Ok(n) => n,
                            Err(e) => {
                                tracing::warn!(
                                    owner = %owner_id,
                                    "coordinated reader: live_consumers query failed, falling back to local count: {e}"
                                );
                                owned.len().max(1)
                            }
                        };
                        let target =
                            target_partition_count(partition_count, live, slack) as usize;

                        if owned.len() < target {
                            for partition_id in 0..partition_count.get() {
                                if owned.len() >= target {
                                    break;
                                }
                                if owned.contains_key(&partition_id) {
                                    continue;
                                }
                                let partition =
                                    match Partition::new(partition_id, partition_count) {
                                        Ok(p) => p,
                                        Err(_) => continue,
                                    };
                                match coordinator
                                    .claim(
                                        &scope,
                                        &owner_id,
                                        partition,
                                        partition_lease_duration,
                                    )
                                    .await
                                {
                                    Ok(Some(lease)) => {
                                        owned.insert(partition_id, lease);
                                        // No need to stop the running pump
                                        // immediately. The pump's inner
                                        // subscription was built with
                                        // `with_partitions(group)` over the
                                        // previously owned set, so the backend
                                        // SQL filter (`partition_id = ANY(...)`)
                                        // physically cannot deliver events for
                                        // this newly-claimed partition. The
                                        // rebuild path below tears down the
                                        // current pump and starts a fresh one
                                        // whose filter includes the new
                                        // partition.
                                        needs_rebuild = true;
                                    }
                                    Ok(None) => {}
                                    Err(e) => {
                                        tracing::warn!(
                                            owner = %owner_id,
                                            partition = partition_id,
                                            "coordinated reader: claim failed: {e}"
                                        );
                                    }
                                }
                            }
                        } else if owned.len() > target {
                            let mut to_release: Vec<u32> =
                                owned.keys().copied().collect();
                            to_release.sort_by(|a, b| b.cmp(a));
                            let surplus = owned.len() - target;
                            let releasing: Vec<u32> =
                                to_release.into_iter().take(surplus).collect();
                            if !releasing.is_empty() {
                                // Stop the current pump before draining
                                // partitions; an active pump may hold
                                // outstanding acks against these leases.
                                stop_pump(pump.take()).await;
                                for partition_id in &releasing {
                                    if let Some(lease) = owned.remove(partition_id) {
                                        flush_pending_for(
                                            &pending_checkpoints,
                                            &coordinator,
                                            &lease,
                                            &owner_id,
                                        )
                                        .await;
                                        if let Err(e) = coordinator.release(&lease).await {
                                            tracing::warn!(
                                                owner = %owner_id,
                                                partition = partition_id,
                                                "coordinated reader: surplus release failed: {e}"
                                            );
                                        }
                                    }
                                    forward_high_water.lock().await.remove(partition_id);
                                }
                                needs_rebuild = true;
                            }
                        }

                        if needs_rebuild {
                            // Tear down the current pump (if any) and start
                            // a fresh one over the new owned set. Idempotent
                            // with the surplus-release branch above, which
                            // already stopped the pump before draining
                            // partitions; `stop_pump(None)` is a no-op.
                            stop_pump(pump.take()).await;
                            pump = build_pump(
                                &inner_reader,
                                &coordinator,
                                &owner_id,
                                &owned,
                                partition_count,
                                base_sub.clone(),
                                start.clone(),
                                lane_capacity,
                                tx.clone(),
                                &pending_checkpoints,
                                &forward_high_water,
                                flush_policy,
                            )
                            .await;
                            needs_rebuild = false;
                        }

                        next_rebalance = tokio::time::Instant::now()
                            + jittered_duration(rebalance_interval);
                    }

                    _ = tokio::time::sleep_until(next_renew) => {
                        let partition_ids: Vec<u32> = owned.keys().copied().collect();
                        let mut lost: Vec<u32> = Vec::new();
                        for partition_id in partition_ids {
                            if let Some(lease) = owned.get(&partition_id) {
                                match coordinator
                                    .renew(lease, partition_lease_duration)
                                    .await
                                {
                                    Ok(()) => {}
                                    Err(Error::OwnershipLost(_)) => {
                                        lost.push(partition_id);
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            owner = %owner_id,
                                            partition = partition_id,
                                            "coordinated reader: renew failed (transient): {e}"
                                        );
                                    }
                                }
                            }
                        }
                        if !lost.is_empty() {
                            // Lost ownership: stop the pump so the inner
                            // stream can't write further fenced checkpoints
                            // for these partitions. Then drop the leases.
                            stop_pump(pump.take()).await;
                            for partition_id in &lost {
                                tracing::info!(
                                    owner = %owner_id,
                                    partition = partition_id,
                                    "coordinated reader: partition ownership lost"
                                );
                                owned.remove(partition_id);
                                pending_checkpoints.lock().await.remove(partition_id);
                                forward_high_water.lock().await.remove(partition_id);
                            }
                            if !owned.is_empty() {
                                pump = build_pump(
                                    &inner_reader,
                                    &coordinator,
                                    &owner_id,
                                    &owned,
                                    partition_count,
                                    base_sub.clone(),
                                    start.clone(),
                                    lane_capacity,
                                    tx.clone(),
                                    &pending_checkpoints,
                                    &forward_high_water,
                                    flush_policy,
                                )
                                .await;
                            }
                        }
                        if let Err(e) = coordinator
                            .heartbeat(&scope, &owner_id, consumer_lease_duration)
                            .await
                        {
                            tracing::warn!(
                                owner = %owner_id,
                                "coordinated reader: consumer heartbeat failed: {e}"
                            );
                        }

                        tick_interval_flush(
                            &pending_checkpoints,
                            &coordinator,
                            &owned,
                            &owner_id,
                            flush_policy.max_pending_interval,
                        )
                        .await;

                        next_renew =
                            tokio::time::Instant::now() + jittered_duration(tick_duration);
                    }

                    _ = shutdown_notify.notified() => {
                        stop_pump(pump.take()).await;
                        release_owned_partitions_concurrent(
                            &mut owned,
                            &coordinator,
                            &pending_checkpoints,
                            &owner_id,
                            "on shutdown",
                        ).await;
                        if let Err(e) = coordinator.release_consumer(&scope, &owner_id).await {
                            tracing::warn!(
                                owner = %owner_id,
                                "coordinated reader: release_consumer on shutdown failed: {e}"
                            );
                        }
                        return;
                    }
                }

                if tx.is_closed() {
                    stop_pump(pump.take()).await;
                    release_owned_partitions_concurrent(
                        &mut owned,
                        &coordinator,
                        &pending_checkpoints,
                        &owner_id,
                        "on stream close",
                    )
                    .await;
                    if let Err(e) = coordinator.release_consumer(&scope, &owner_id).await {
                        tracing::warn!(
                            owner = %owner_id,
                            "coordinated reader: release_consumer on stream close failed: {e}"
                        );
                    }
                    return;
                }
            }
        });

        Ok(CoordinatedStream { rx, shutdown })
    }
}

/// Spawn the inner-stream pump for the current `owned` set.
///
/// Returns `None` if `owned` is empty or if the inner reader fails to start.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::type_complexity)]
async fn build_pump<R, S, C, A, Coord, P>(
    inner_reader: &R,
    coordinator: &Arc<Coord>,
    owner_id: &OwnerId,
    owned: &HashMap<u32, PartitionLease<PartitionedCursor<C>>>,
    partition_count: NonZeroU32,
    base_sub: S,
    start: StartFrom<C>,
    lane_capacity: NonZeroUsize,
    tx: mpsc::Sender<
        Result<
            Message<
                CoordinatedAcker<PartitionAcker<A, C, P>, PartitionedCursor<C>, Coord>,
                CoordinatedCursor<PartitionedCursor<C>>,
                P,
            >,
        >,
    >,
    pending: &PendingCheckpoints<PartitionedCursor<C>>,
    forward_high_water: &Arc<Mutex<HashMap<u32, PartitionedCursor<C>>>>,
    flush_policy: CheckpointFlushPolicy,
) -> Option<(tokio::task::JoinHandle<()>, CancellationToken)>
where
    R: Reader<P, Subscription = S, Cursor = C, Acker = A> + Clone + Send + Sync + 'static,
    S: PartitionableSubscription<C> + Clone + Send + Sync + 'static,
    C: Cursor + HasPartition + Clone + Ord + Send + Sync + 'static,
    A: Acker + Clone + Send + Sync + 'static,
    R::Stream: Send + 'static,
    Coord: PartitionCoordinator<PartitionedCursor<C>>,
    P: Clone + Send + Sync + 'static,
{
    if owned.is_empty() {
        return None;
    }

    let partitions: std::result::Result<Vec<Partition>, _> = owned
        .keys()
        .copied()
        .map(|id| Partition::new(id, partition_count))
        .collect();
    let partitions = match partitions {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(
                owner = %owner_id,
                "coordinated reader: failed to construct owned partition list: {e}"
            );
            return None;
        }
    };
    let group = match PartitionGroup::new(partitions) {
        Ok(g) => g,
        Err(e) => {
            tracing::warn!(
                owner = %owner_id,
                "coordinated reader: failed to build owned PartitionGroup: {e}"
            );
            return None;
        }
    };

    let inner_sub = base_sub.with_partitions(group);

    let scheduler = PartitionedReader::source_from_cursor(
        inner_reader.clone(),
        PartitionedReaderConfig {
            partition_count,
            lane_capacity,
            ..PartitionedReaderConfig::default()
        },
    );

    // Inner-stream start position:
    //
    // We can only express ONE start position for the shared SQL fetch. If
    // every owned partition has a checkpoint, take the minimum of those as
    // a sound lower bound (events below that are guaranteed acked on every
    // owned partition). Otherwise fall back to the subscription's `start`:
    // a partition with no checkpoint must see its earliest events, and a
    // single global lower bound can't satisfy both "advance past checkpoint
    // for some" and "include everything for the rest".
    //
    // The pump task additionally filters per-partition below to drop any
    // event whose `order_key()` is <= that partition's checkpoint. The
    // `PartitionedReader` source-cursor mode acks the inner reader on lane
    // accept, so source-level progress still advances even for dropped
    // events.
    let all_partitions_checkpointed =
        !owned.is_empty() && owned.values().all(|l| l.checkpoint_cursor.is_some());
    let scheduler_sub: PartitionedSubscription<S, C> = if all_partitions_checkpointed {
        let starts: Vec<StartFrom<PartitionedCursor<C>>> = build_inner_starts(owned);
        PartitionedSubscription::new(inner_sub).with_starts(starts)
    } else {
        let outer_start: StartFrom<PartitionedCursor<C>> = match start {
            StartFrom::Earliest => StartFrom::Earliest,
            StartFrom::Latest => StartFrom::Latest,
            StartFrom::Timestamp(t) => StartFrom::Timestamp(t),
            // Untranslatable: a raw inner cursor without a partition. Fall
            // back to Earliest; callers expressing this through
            // `CoordinatedSubscription::start` should rely on per-partition
            // checkpoints instead.
            StartFrom::After(_) => StartFrom::Earliest,
        };
        PartitionedSubscription::new(inner_sub).with_start(outer_start)
    };

    let inner_stream = match scheduler.read(scheduler_sub).await {
        Ok(s) => s,
        Err(e) => {
            let _ = tx.send(Err(e)).await;
            return None;
        }
    };

    let cancel = CancellationToken::new();
    let pump_cancel = cancel.clone();
    let coord = Arc::clone(coordinator);
    let owner_id_for_pump = owner_id.clone();
    let pending = Arc::clone(pending);
    let high_water = Arc::clone(forward_high_water);
    let leases_by_partition: HashMap<u32, PartitionLease<PartitionedCursor<C>>> = owned.clone();

    let handle = tokio::spawn(async move {
        tokio::pin!(inner_stream);
        loop {
            tokio::select! {
                biased;
                _ = pump_cancel.cancelled() => {
                    return;
                }
                item = inner_stream.next() => {
                    let Some(item) = item else { return; };
                    match item {
                        Ok(msg) => {
                            let (event, inner_acker, inner_cursor) = msg.into_parts();
                            let partition_id = inner_cursor.partition().id();
                            let Some(lease) = leases_by_partition.get(&partition_id) else {
                                // Inner stream produced a message for a
                                // partition not in this pump's lease snapshot.
                                // This is a teardown-race artifact: the
                                // surplus-release / lost-ownership paths stop
                                // the pump before mutating `owned`, but the
                                // lane scheduler may still drain a few
                                // in-flight items from its lanes after
                                // cancellation. Ack the lane-scheduler acker
                                // so its lane unblocks for teardown.
                                //
                                // The event is not lost: we never invoke the
                                // coordinator-side acker, so this partition's
                                // checkpoint is not advanced. If we reclaim
                                // the partition on a later rebalance the next
                                // pump re-fetches from the durable checkpoint;
                                // if not, the partition is owned by another
                                // consumer and the event is theirs to deliver.
                                tracing::warn!(
                                    owner = %owner_id_for_pump,
                                    partition = partition_id,
                                    "coordinated reader: dropping message for unowned partition"
                                );
                                let _ = inner_acker.ack().await;
                                continue;
                            };
                            // Per-partition resume: the shared inner stream
                            // is started from the minimum cursor across owned
                            // partitions (the only thing a single SQL filter
                            // can express). Events below this partition's
                            // checkpoint must therefore be dropped here. The
                            // inner acker was already invoked on lane accept,
                            // so source-cursor progress is preserved.
                            // Two-tier per-partition resume filter:
                            //  1) `lease.checkpoint_cursor` — durable
                            //     coordinator checkpoint at the time this
                            //     lease was claimed. Events at or below
                            //     this were acked by a prior owner.
                            //  2) `forward_high_water[p]` — cursor of the
                            //     highest event this owner has forwarded
                            //     downstream across all previous pump
                            //     incarnations. Survives pump rebuild.
                            //
                            // If either says "already delivered", drop the
                            // event. Ack the lane-scheduler acker so the
                            // lane unblocks (the in-flight slot must clear
                            // before the next event in this lane can be
                            // emitted).
                            let mut skip = false;
                            if let Some(checkpoint) = lease.checkpoint_cursor.as_ref()
                                && inner_cursor.order_key() <= checkpoint.order_key()
                            {
                                skip = true;
                            }
                            if !skip
                                && let Some(prev) = high_water.lock().await.get(&partition_id)
                                && inner_cursor.order_key() <= prev.order_key()
                            {
                                skip = true;
                            }
                            if skip {
                                if let Err(e) = inner_acker.ack().await {
                                    tracing::warn!(
                                        owner = %owner_id_for_pump,
                                        partition = partition_id,
                                        "coordinated reader: filter-ack on inner acker failed: {e}"
                                    );
                                }
                                continue;
                            }
                            // Record this event as delivered before
                            // forwarding so a subsequent rebuild sees the
                            // updated high-water.
                            {
                                let mut hw = high_water.lock().await;
                                let should_insert = hw
                                    .get(&partition_id)
                                    .map(|prev| inner_cursor.order_key() > prev.order_key())
                                    .unwrap_or(true);
                                if should_insert {
                                    hw.insert(partition_id, inner_cursor.clone());
                                }
                            }
                            let coord_acker = CoordinatedAcker::with_buffer(
                                inner_acker,
                                Arc::clone(&coord),
                                lease.clone(),
                                inner_cursor.clone(),
                                Arc::clone(&pending),
                                flush_policy,
                            );
                            let coord_cursor =
                                CoordinatedCursor::new(lease.partition, inner_cursor);
                            let out = Message::new(event, coord_acker, coord_cursor);
                            if tx.send(Ok(out)).await.is_err() {
                                return;
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(Err(e)).await;
                            return;
                        }
                    }
                }
            }
        }
    });

    Some((handle, cancel))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::acker::NoopAcker;
    use crate::io::position::StartableSubscription;
    use crate::io::{ConsumerGroupId, StreamId};
    use crate::partition::PartitionGroup;
    use futures::stream;

    #[test]
    fn generation_initial_is_zero() {
        assert_eq!(Generation::initial().get(), 0);
    }

    #[test]
    fn generation_next_increments() {
        let g = Generation::initial();
        assert_eq!(g.next().get(), 1);
        assert_eq!(g.next().next().get(), 2);
    }

    #[test]
    fn generation_ordering() {
        assert!(Generation::initial() < Generation::initial().next());
        assert!(Generation::from_i64(5) > Generation::from_i64(3));
    }

    #[test]
    fn generation_serde_transparent() {
        let g = Generation::from_i64(7);
        let v = serde_json::to_value(g).unwrap();
        assert_eq!(v, serde_json::json!(7));
        let back: Generation = serde_json::from_value(v).unwrap();
        assert_eq!(back, g);
    }

    #[test]
    fn error_ownership_lost_variant_exists() {
        let err = crate::Error::OwnershipLost("test".to_owned());
        assert!(matches!(err, crate::Error::OwnershipLost(_)));
    }

    #[test]
    fn partition_lease_constructs_and_equals() {
        let scope = CheckpointScope::new(
            ConsumerGroupId::new("my-group").unwrap(),
            StreamId::new("orders").unwrap(),
        );
        let partition = Partition::new(3, NonZeroU32::new(8).unwrap()).unwrap();
        let lease: PartitionLease<i64> = PartitionLease {
            scope,
            owner_id: OwnerId::new("worker-01").unwrap(),
            partition,
            generation: Generation::from_i64(1),
            checkpoint_cursor: Some(42),
            lease_until: DateTime::from_timestamp(0, 0).unwrap(),
        };
        let cloned = lease.clone();
        assert_eq!(lease, cloned);
    }

    #[test]
    fn target_partition_count_single_consumer() {
        let total = NonZeroU32::new(8).unwrap();
        assert_eq!(target_partition_count(total, 1, 0), 8);
    }

    #[test]
    fn target_partition_count_two_consumers() {
        let total = NonZeroU32::new(8).unwrap();
        assert_eq!(target_partition_count(total, 2, 0), 4);
    }

    #[test]
    fn target_partition_count_three_consumers_no_slack() {
        let total = NonZeroU32::new(8).unwrap();
        assert_eq!(target_partition_count(total, 3, 0), 3);
    }

    #[test]
    fn target_partition_count_three_consumers_with_slack() {
        let total = NonZeroU32::new(8).unwrap();
        assert_eq!(target_partition_count(total, 3, 1), 4);
    }

    #[test]
    fn target_partition_count_zero_consumers_treated_as_one() {
        let total = NonZeroU32::new(8).unwrap();
        assert_eq!(target_partition_count(total, 0, 0), 8);
    }

    #[test]
    fn coordinated_reader_config_default_has_no_partition_slack() {
        assert_eq!(CoordinatedReaderConfig::default().partition_slack, 0);
    }

    #[test]
    fn jittered_duration_zero_base_returns_zero() {
        assert_eq!(jittered_duration(Duration::ZERO), Duration::ZERO);
    }

    #[test]
    fn jittered_duration_100ms_stays_in_range() {
        let base = Duration::from_millis(100);
        let max = Duration::from_millis(120);
        for _ in 0..20 {
            let result = jittered_duration(base);
            assert!(
                result >= base && result <= max,
                "jittered_duration({base:?}) = {result:?} not in [100ms, 120ms]"
            );
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestPayload {
        value: String,
    }

    #[derive(
        Debug,
        Clone,
        Copy,
        Eq,
        PartialEq,
        Ord,
        PartialOrd,
        Hash,
        serde::Serialize,
        serde::Deserialize,
    )]
    struct StampedCursor {
        sequence: i64,
        partition: Partition,
    }

    impl Cursor for StampedCursor {
        fn order_key(&self) -> CursorOrder {
            CursorOrder::from_i64(self.sequence)
        }
    }

    impl HasPartition for StampedCursor {
        fn partition(&self) -> Partition {
            self.partition
        }
    }

    #[derive(Debug, Clone, Default)]
    struct TypedSubscription {
        start: StartFrom<StampedCursor>,
        partition: Option<Partition>,
    }

    impl StartableSubscription<StampedCursor> for TypedSubscription {
        fn with_start(mut self, start: StartFrom<StampedCursor>) -> Self {
            self.start = start;
            self
        }
    }

    impl PartitionableSubscription<StampedCursor> for TypedSubscription {
        fn with_partitions(mut self, group: PartitionGroup) -> Self {
            self.partition = group.partitions().first().copied();
            self
        }
    }

    #[derive(Clone)]
    struct TypedOneShotReader;

    impl Reader<TestPayload> for TypedOneShotReader {
        type Subscription = TypedSubscription;
        type Acker = NoopAcker;
        type Cursor = StampedCursor;
        type Stream = Pin<
            Box<dyn Stream<Item = Result<Message<NoopAcker, StampedCursor, TestPayload>>> + Send>,
        >;

        async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
            let partition = subscription.partition.expect("partition was set");
            let event = crate::Event::create(
                "acme",
                "/typed",
                "typed.delivered",
                "thing-1",
                TestPayload {
                    value: "typed-value".to_owned(),
                },
            )?;
            Ok(Box::pin(stream::once(async move {
                Ok(Message::new(
                    event,
                    NoopAcker,
                    StampedCursor {
                        sequence: 1,
                        partition,
                    },
                ))
            })))
        }
    }

    #[derive(Debug, Default)]
    struct TestCoordinatorState {
        consumers: HashMap<(CheckpointScope, OwnerId), DateTime<Utc>>,
        partitions: HashMap<(CheckpointScope, u32), TestPartitionState>,
    }

    #[derive(Debug, Clone)]
    struct TestPartitionState {
        owner_id: Option<OwnerId>,
        lease_until: Option<DateTime<Utc>>,
        generation: Generation,
        checkpoint_cursor: Option<StampedCursor>,
    }

    impl Default for TestPartitionState {
        fn default() -> Self {
            Self {
                owner_id: None,
                lease_until: None,
                generation: Generation::initial(),
                checkpoint_cursor: None,
            }
        }
    }

    #[derive(Debug, Default)]
    struct TestCoordinator {
        state: Arc<tokio::sync::Mutex<TestCoordinatorState>>,
    }

    impl TestCoordinator {
        fn new() -> Self {
            Self::default()
        }
    }

    impl Clone for TestCoordinator {
        fn clone(&self) -> Self {
            Self {
                state: Arc::clone(&self.state),
            }
        }
    }

    impl PartitionCoordinator<StampedCursor> for TestCoordinator {
        async fn heartbeat<'a>(
            &'a self,
            scope: &'a CheckpointScope,
            owner_id: &'a OwnerId,
            lease_duration: Duration,
        ) -> Result<()> {
            let lease_until = Utc::now() + lease_duration;
            self.state
                .lock()
                .await
                .consumers
                .insert((scope.clone(), owner_id.clone()), lease_until);
            Ok(())
        }

        async fn live_consumers<'a>(&'a self, scope: &'a CheckpointScope) -> Result<usize> {
            let now = Utc::now();
            let state = self.state.lock().await;
            Ok(state
                .consumers
                .iter()
                .filter(|((s, _), lease_until)| s == scope && **lease_until > now)
                .count())
        }

        async fn release_consumer<'a>(
            &'a self,
            scope: &'a CheckpointScope,
            owner_id: &'a OwnerId,
        ) -> Result<()> {
            self.state
                .lock()
                .await
                .consumers
                .remove(&(scope.clone(), owner_id.clone()));
            Ok(())
        }

        async fn claim<'a>(
            &'a self,
            scope: &'a CheckpointScope,
            owner_id: &'a OwnerId,
            partition: Partition,
            lease_duration: Duration,
        ) -> Result<Option<PartitionLease<StampedCursor>>> {
            let now = Utc::now();
            let lease_until = now + lease_duration;
            let key = (scope.clone(), partition.id());
            let mut state = self.state.lock().await;
            let entry = state.partitions.entry(key).or_default();
            let is_expired = entry.lease_until.map(|t| t <= now).unwrap_or(true);
            let is_unowned = entry.owner_id.is_none();
            let is_self = entry.owner_id.as_ref() == Some(owner_id);
            if is_unowned || is_expired || is_self {
                entry.generation = entry.generation.next();
                entry.owner_id = Some(owner_id.clone());
                entry.lease_until = Some(lease_until);
                Ok(Some(PartitionLease {
                    scope: scope.clone(),
                    owner_id: owner_id.clone(),
                    partition,
                    generation: entry.generation,
                    checkpoint_cursor: entry.checkpoint_cursor,
                    lease_until,
                }))
            } else {
                Ok(None)
            }
        }

        async fn renew<'a>(
            &'a self,
            lease: &'a PartitionLease<StampedCursor>,
            lease_duration: Duration,
        ) -> Result<()> {
            let key = (lease.scope.clone(), lease.partition.id());
            let mut state = self.state.lock().await;
            match state.partitions.get_mut(&key) {
                Some(entry)
                    if entry.owner_id.as_ref() == Some(&lease.owner_id)
                        && entry.generation == lease.generation =>
                {
                    entry.lease_until = Some(Utc::now() + lease_duration);
                    Ok(())
                }
                _ => Err(Error::OwnershipLost(format!(
                    "partition {}: ownership lost",
                    lease.partition.id()
                ))),
            }
        }

        async fn release<'a>(&'a self, lease: &'a PartitionLease<StampedCursor>) -> Result<()> {
            let key = (lease.scope.clone(), lease.partition.id());
            let mut state = self.state.lock().await;
            match state.partitions.get_mut(&key) {
                Some(entry)
                    if entry.owner_id.as_ref() == Some(&lease.owner_id)
                        && entry.generation == lease.generation =>
                {
                    entry.owner_id = None;
                    entry.lease_until = None;
                    entry.generation = entry.generation.next();
                    Ok(())
                }
                _ => Err(Error::OwnershipLost(format!(
                    "partition {}: ownership lost",
                    lease.partition.id()
                ))),
            }
        }

        async fn checkpoint<'a>(
            &'a self,
            lease: &'a PartitionLease<StampedCursor>,
            cursor: StampedCursor,
        ) -> Result<()> {
            let key = (lease.scope.clone(), lease.partition.id());
            let mut state = self.state.lock().await;
            match state.partitions.get_mut(&key) {
                Some(entry)
                    if entry.owner_id.as_ref() == Some(&lease.owner_id)
                        && entry.generation == lease.generation =>
                {
                    entry.checkpoint_cursor = Some(cursor);
                    Ok(())
                }
                _ => Err(Error::OwnershipLost(format!(
                    "partition {}: ownership lost",
                    lease.partition.id()
                ))),
            }
        }
    }

    #[tokio::test]
    async fn coordinated_reader_preserves_typed_payloads() {
        let coordinator = Arc::new(TestCoordinator::new());
        let reader = CoordinatedReader::new(
            TypedOneShotReader,
            coordinator,
            OwnerId::new("typed-owner").unwrap(),
            CoordinatedReaderConfig {
                rebalance_interval: Duration::from_millis(10),
                partition_lease_duration: Duration::from_secs(1),
                partition_renew_interval: Duration::from_millis(100),
                consumer_lease_duration: Duration::from_secs(1),
                consumer_heartbeat_interval: Duration::from_millis(100),
                partition_slack: 0,
                checkpoint_flush: CheckpointFlushPolicy::default(),
                ..CoordinatedReaderConfig::default()
            },
        );

        let sub = CoordinatedSubscription {
            inner: TypedSubscription::default(),
            scope: CheckpointScope::new(
                ConsumerGroupId::new("typed-group").unwrap(),
                StreamId::new("typed-stream").unwrap(),
            ),
            partition_count: NonZeroU32::new(1).unwrap(),
            start: StartFrom::Earliest,
        };

        let mut stream = Reader::<TestPayload>::read(&reader, sub).await.unwrap();
        let msg = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(msg.event().payload().value, "typed-value");
        msg.ack().await.unwrap();
    }

    #[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
    struct CountCursor(u64);

    impl Cursor for CountCursor {
        fn id(&self) -> CursorId {
            CursorId::global()
        }

        fn order_key(&self) -> CursorOrder {
            CursorOrder::from_u64(self.0)
        }
    }

    #[derive(Debug, Default)]
    struct CountingCoordinatorState {
        checkpoint_calls: u64,
        last_cursor: Option<CountCursor>,
    }

    #[derive(Clone, Default)]
    struct CountingCoordinator {
        state: Arc<tokio::sync::Mutex<CountingCoordinatorState>>,
    }

    impl CountingCoordinator {
        async fn snapshot(&self) -> (u64, Option<CountCursor>) {
            let s = self.state.lock().await;
            (s.checkpoint_calls, s.last_cursor)
        }
    }

    impl PartitionCoordinator<CountCursor> for CountingCoordinator {
        async fn heartbeat<'a>(
            &'a self,
            _scope: &'a CheckpointScope,
            _owner_id: &'a OwnerId,
            _lease_duration: Duration,
        ) -> Result<()> {
            Ok(())
        }

        async fn live_consumers<'a>(&'a self, _scope: &'a CheckpointScope) -> Result<usize> {
            Ok(1)
        }

        async fn release_consumer<'a>(
            &'a self,
            _scope: &'a CheckpointScope,
            _owner_id: &'a OwnerId,
        ) -> Result<()> {
            Ok(())
        }

        async fn claim<'a>(
            &'a self,
            _scope: &'a CheckpointScope,
            _owner_id: &'a OwnerId,
            _partition: Partition,
            _lease_duration: Duration,
        ) -> Result<Option<PartitionLease<CountCursor>>> {
            Ok(None)
        }

        async fn renew<'a>(
            &'a self,
            _lease: &'a PartitionLease<CountCursor>,
            _lease_duration: Duration,
        ) -> Result<()> {
            Ok(())
        }

        async fn release<'a>(&'a self, _lease: &'a PartitionLease<CountCursor>) -> Result<()> {
            Ok(())
        }

        async fn checkpoint<'a>(
            &'a self,
            _lease: &'a PartitionLease<CountCursor>,
            cursor: CountCursor,
        ) -> Result<()> {
            let mut s = self.state.lock().await;
            s.checkpoint_calls += 1;
            s.last_cursor = Some(cursor);
            Ok(())
        }
    }

    fn test_lease() -> PartitionLease<CountCursor> {
        PartitionLease {
            scope: CheckpointScope::new(
                ConsumerGroupId::new("flush-group").unwrap(),
                StreamId::new("flush-stream").unwrap(),
            ),
            owner_id: OwnerId::new("flush-owner").unwrap(),
            partition: Partition::new(0, NonZeroU32::new(4).unwrap()).unwrap(),
            generation: Generation::initial(),
            checkpoint_cursor: None,
            lease_until: Utc::now() + Duration::from_secs(60),
        }
    }

    fn build_buffered_acker(
        coordinator: Arc<CountingCoordinator>,
        pending: PendingCheckpoints<CountCursor>,
        policy: CheckpointFlushPolicy,
        cursor: CountCursor,
    ) -> CoordinatedAcker<NoopAcker, CountCursor, CountingCoordinator> {
        CoordinatedAcker::with_buffer(
            NoopAcker,
            coordinator,
            test_lease(),
            cursor,
            pending,
            policy,
        )
    }

    #[tokio::test]
    async fn flush_policy_default_flushes_on_every_ack() {
        let coordinator = Arc::new(CountingCoordinator::default());
        let pending: PendingCheckpoints<CountCursor> = Arc::new(Mutex::new(HashMap::new()));
        let policy = CheckpointFlushPolicy::default();

        for i in 1..=3 {
            let acker = build_buffered_acker(
                Arc::clone(&coordinator),
                Arc::clone(&pending),
                policy,
                CountCursor(i),
            );
            acker.ack().await.unwrap();
        }

        let (calls, last) = coordinator.snapshot().await;
        assert_eq!(calls, 3);
        assert_eq!(last, Some(CountCursor(3)));
        assert!(pending.lock().await.is_empty());
    }

    #[tokio::test]
    async fn flush_policy_count_5_flushes_only_on_fifth_ack() {
        let coordinator = Arc::new(CountingCoordinator::default());
        let pending: PendingCheckpoints<CountCursor> = Arc::new(Mutex::new(HashMap::new()));
        let policy = CheckpointFlushPolicy {
            max_pending_acks: NonZeroUsize::new(5).unwrap(),
            max_pending_interval: Duration::ZERO,
        };

        for i in 1..=4 {
            let acker = build_buffered_acker(
                Arc::clone(&coordinator),
                Arc::clone(&pending),
                policy,
                CountCursor(i),
            );
            acker.ack().await.unwrap();
        }
        let (calls, _) = coordinator.snapshot().await;
        assert_eq!(calls, 0, "first 4 acks must not flush");

        let acker = build_buffered_acker(
            Arc::clone(&coordinator),
            Arc::clone(&pending),
            policy,
            CountCursor(5),
        );
        acker.ack().await.unwrap();

        let (calls, last) = coordinator.snapshot().await;
        assert_eq!(calls, 1);
        assert_eq!(last, Some(CountCursor(5)));
        assert!(pending.lock().await.is_empty());
    }

    #[tokio::test]
    async fn flush_policy_keeps_highest_cursor_in_buffer() {
        let coordinator = Arc::new(CountingCoordinator::default());
        let pending: PendingCheckpoints<CountCursor> = Arc::new(Mutex::new(HashMap::new()));
        let policy = CheckpointFlushPolicy {
            max_pending_acks: NonZeroUsize::new(3).unwrap(),
            max_pending_interval: Duration::ZERO,
        };

        for cursor in [CountCursor(10), CountCursor(7), CountCursor(99)] {
            let acker = build_buffered_acker(
                Arc::clone(&coordinator),
                Arc::clone(&pending),
                policy,
                cursor,
            );
            acker.ack().await.unwrap();
        }

        let (calls, last) = coordinator.snapshot().await;
        assert_eq!(calls, 1);
        assert_eq!(last, Some(CountCursor(99)));
    }

    #[tokio::test]
    async fn flush_policy_interval_flushes_when_threshold_exceeded() {
        let coordinator = Arc::new(CountingCoordinator::default());
        let pending: PendingCheckpoints<CountCursor> = Arc::new(Mutex::new(HashMap::new()));
        let policy = CheckpointFlushPolicy {
            max_pending_acks: NonZeroUsize::new(100).unwrap(),
            max_pending_interval: Duration::from_millis(50),
        };

        let acker = build_buffered_acker(
            Arc::clone(&coordinator),
            Arc::clone(&pending),
            policy,
            CountCursor(1),
        );
        acker.ack().await.unwrap();
        assert_eq!(coordinator.snapshot().await.0, 0);

        tokio::time::sleep(Duration::from_millis(250)).await;

        let acker = build_buffered_acker(
            Arc::clone(&coordinator),
            Arc::clone(&pending),
            policy,
            CountCursor(2),
        );
        acker.ack().await.unwrap();

        let (calls, last) = coordinator.snapshot().await;
        assert_eq!(calls, 1);
        assert_eq!(last, Some(CountCursor(2)));
    }

    #[tokio::test]
    async fn flush_pending_for_drains_buffer_on_release() {
        let coordinator = Arc::new(CountingCoordinator::default());
        let pending: PendingCheckpoints<CountCursor> = Arc::new(Mutex::new(HashMap::new()));
        let policy = CheckpointFlushPolicy {
            max_pending_acks: NonZeroUsize::new(10).unwrap(),
            max_pending_interval: Duration::ZERO,
        };

        let acker = build_buffered_acker(
            Arc::clone(&coordinator),
            Arc::clone(&pending),
            policy,
            CountCursor(42),
        );
        acker.ack().await.unwrap();
        assert_eq!(coordinator.snapshot().await.0, 0);

        let lease = test_lease();
        let owner = OwnerId::new("flush-owner").unwrap();
        flush_pending_for(&pending, &coordinator, &lease, &owner).await;

        let (calls, last) = coordinator.snapshot().await;
        assert_eq!(calls, 1);
        assert_eq!(last, Some(CountCursor(42)));
        assert!(pending.lock().await.is_empty());
    }

    #[tokio::test]
    async fn tick_interval_flush_flushes_due_partition() {
        let coordinator = Arc::new(CountingCoordinator::default());
        let pending: PendingCheckpoints<CountCursor> = Arc::new(Mutex::new(HashMap::new()));
        let policy = CheckpointFlushPolicy {
            max_pending_acks: NonZeroUsize::new(100).unwrap(),
            max_pending_interval: Duration::from_millis(20),
        };

        let acker = build_buffered_acker(
            Arc::clone(&coordinator),
            Arc::clone(&pending),
            policy,
            CountCursor(7),
        );
        acker.ack().await.unwrap();

        tokio::time::sleep(Duration::from_millis(40)).await;

        let mut owned: HashMap<u32, PartitionLease<CountCursor>> = HashMap::new();
        owned.insert(0, test_lease());

        let owner = OwnerId::new("flush-owner").unwrap();
        tick_interval_flush(
            &pending,
            &coordinator,
            &owned,
            &owner,
            policy.max_pending_interval,
        )
        .await;

        let (calls, last) = coordinator.snapshot().await;
        assert_eq!(calls, 1);
        assert_eq!(last, Some(CountCursor(7)));
        assert!(pending.lock().await.is_empty());
    }
}
