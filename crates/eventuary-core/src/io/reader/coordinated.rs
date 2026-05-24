//! Generic coordinated reader: distributes partition ownership across instances.
//!
//! `CoordinatedReader<R, Coord>` wraps an inner partition-aware `Reader` with
//! a `PartitionCoordinator<C>` to provide multi-instance partition leasing,
//! dynamic rebalance, lease renewal, monotonic checkpointing, and graceful
//! release on shutdown. Backend crates (`eventuary-postgres`,
//! `eventuary-sqlite`) ship as thin `pub type` aliases over this generic.

use std::collections::HashMap;
use std::num::NonZeroU16;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Stream;
use futures::StreamExt;
use rand::Rng;
use tokio::sync::{Notify, mpsc};

use crate::Result;
use crate::error::Error;
use crate::event_key::Partition;
use crate::io::partition_coordinator::{PartitionCoordinator, PartitionLease};
use crate::io::partitionable_subscription::PartitionableSubscription;
use crate::io::position::StartFrom;
use crate::io::reader::CheckpointScope;
use crate::io::{Acker, Cursor, CursorId, CursorOrder, Message, OwnerId, Reader};

#[derive(Debug, Clone, Copy)]
pub struct CoordinatedReaderConfig {
    pub partition_lease_duration: Duration,
    pub partition_renew_interval: Duration,
    pub consumer_lease_duration: Duration,
    pub consumer_heartbeat_interval: Duration,
    pub rebalance_interval: Duration,
    pub partition_slack: u16,
}

impl Default for CoordinatedReaderConfig {
    fn default() -> Self {
        Self {
            partition_lease_duration: Duration::from_secs(60),
            partition_renew_interval: Duration::from_secs(15),
            consumer_lease_duration: Duration::from_secs(30),
            consumer_heartbeat_interval: Duration::from_secs(10),
            rebalance_interval: Duration::from_secs(10),
            partition_slack: 1,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CoordinatedSubscription<S, C> {
    pub inner: S,
    pub scope: CheckpointScope,
    pub partition_count: NonZeroU16,
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
        CursorId::partition(self.partition.count(), self.partition.id())
    }

    fn order_key(&self) -> CursorOrder {
        self.inner.order_key()
    }
}

pub struct CoordinatedAcker<A, C, Coord> {
    inner: A,
    coordinator: Arc<Coord>,
    lease: PartitionLease<C>,
    cursor: C,
}

impl<A, C, Coord> CoordinatedAcker<A, C, Coord> {
    pub fn new(inner: A, coordinator: Arc<Coord>, lease: PartitionLease<C>, cursor: C) -> Self {
        Self {
            inner,
            coordinator,
            lease,
            cursor,
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
        self.coordinator
            .checkpoint(&self.lease, self.cursor.clone())
            .await
    }

    async fn nack(&self) -> Result<()> {
        self.inner.nack().await
    }
}

pub struct CoordinatedStream<A, C, Coord>
where
    A: Acker + Send + Sync + 'static,
    C: Cursor + Clone + Send + Sync + 'static,
    Coord: PartitionCoordinator<C>,
{
    #[allow(clippy::type_complexity)]
    rx: mpsc::Receiver<Result<Message<CoordinatedAcker<A, C, Coord>, CoordinatedCursor<C>>>>,
    shutdown: Arc<Notify>,
}

impl<A, C, Coord> Stream for CoordinatedStream<A, C, Coord>
where
    A: Acker + Send + Sync + 'static,
    C: Cursor + Clone + Send + Sync + 'static,
    Coord: PartitionCoordinator<C>,
{
    type Item = Result<Message<CoordinatedAcker<A, C, Coord>, CoordinatedCursor<C>>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

impl<A, C, Coord> Drop for CoordinatedStream<A, C, Coord>
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

fn target_partition_count(total: NonZeroU16, live: usize, slack: u16) -> u16 {
    let total = total.get();
    let live = live.max(1) as u64;
    let base = (total as u64).div_ceil(live) as u16;
    base.saturating_add(slack).min(total)
}

#[allow(clippy::type_complexity)]
async fn partition_worker<R, S, C, A, Coord>(
    inner_reader: R,
    coordinator: Arc<Coord>,
    lease: PartitionLease<C>,
    base_subscription: S,
    start: StartFrom<C>,
    tx: mpsc::Sender<Result<Message<CoordinatedAcker<A, C, Coord>, CoordinatedCursor<C>>>>,
) where
    R: Reader<Subscription = S, Cursor = C, Acker = A>,
    S: PartitionableSubscription<C>,
    C: Cursor + Clone + Send + Sync + 'static,
    A: Acker + Send + Sync + 'static,
    Coord: PartitionCoordinator<C>,
{
    let inner_start = match &lease.checkpoint_cursor {
        Some(cursor) => StartFrom::After(cursor.clone()),
        None => start,
    };
    let inner_sub = base_subscription
        .with_partition(lease.partition)
        .with_start(inner_start);

    let inner_stream = match inner_reader.read(inner_sub).await {
        Ok(s) => s,
        Err(e) => {
            let _ = tx.send(Err(e)).await;
            return;
        }
    };
    tokio::pin!(inner_stream);

    while let Some(item) = inner_stream.next().await {
        match item {
            Ok(msg) => {
                let (event, inner_acker, inner_cursor) = msg.into_parts();
                let coord_acker = CoordinatedAcker::new(
                    inner_acker,
                    Arc::clone(&coordinator),
                    lease.clone(),
                    inner_cursor.clone(),
                );
                let coord_cursor = CoordinatedCursor::new(lease.partition, inner_cursor);
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

impl<R, S, C, A, Coord> Reader for CoordinatedReader<R, Coord>
where
    R: Reader<Subscription = S, Cursor = C, Acker = A> + Clone + Send + Sync + 'static,
    S: PartitionableSubscription<C> + Send + 'static,
    C: Cursor + Clone + Send + Sync + 'static,
    A: Acker + Send + Sync + 'static,
    Coord: PartitionCoordinator<C>,
{
    type Subscription = CoordinatedSubscription<S, C>;
    type Acker = CoordinatedAcker<A, C, Coord>;
    type Cursor = CoordinatedCursor<C>;
    type Stream = CoordinatedStream<A, C, Coord>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        self.coordinator
            .heartbeat(
                &subscription.scope,
                &self.owner_id,
                self.config.consumer_lease_duration,
            )
            .await?;

        let (tx, rx) = mpsc::channel::<
            Result<Message<CoordinatedAcker<A, C, Coord>, CoordinatedCursor<C>>>,
        >(16);
        let shutdown = Arc::new(Notify::new());

        let inner_reader = self.inner.clone();
        let coordinator = Arc::clone(&self.coordinator);
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
        let shutdown_notify = Arc::clone(&shutdown);

        tokio::spawn(async move {
            let mut owned: HashMap<u16, (PartitionLease<C>, tokio::task::JoinHandle<()>)> =
                HashMap::new();

            let tick_duration = renew_interval.min(heartbeat_interval);

            let mut next_rebalance =
                tokio::time::Instant::now() + jittered_duration(rebalance_interval);
            let mut next_renew = tokio::time::Instant::now() + jittered_duration(tick_duration);

            loop {
                tokio::select! {
                    _ = tokio::time::sleep_until(next_rebalance) => {
                        let dead_partitions: Vec<u16> = owned
                            .iter()
                            .filter(|(_, (_, h))| h.is_finished())
                            .map(|(id, _)| *id)
                            .collect();
                        for partition_id in dead_partitions {
                            if let Some((lease, handle)) = owned.remove(&partition_id) {
                                handle.abort();
                                if let Err(e) = coordinator.release(&lease).await {
                                    tracing::warn!(
                                        owner = %owner_id,
                                        partition = partition_id,
                                        "coordinated reader: release of dead worker failed: {e}"
                                    );
                                }
                            }
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
                                        let worker_handle = tokio::spawn(partition_worker(
                                            inner_reader.clone(),
                                            Arc::clone(&coordinator),
                                            lease.clone(),
                                            base_sub.clone(),
                                            start.clone(),
                                            tx.clone(),
                                        ));
                                        owned.insert(partition_id, (lease, worker_handle));
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
                            let mut to_release: Vec<u16> =
                                owned.keys().copied().collect();
                            to_release.sort_by(|a, b| b.cmp(a));
                            let surplus = owned.len() - target;
                            for partition_id in to_release.into_iter().take(surplus) {
                                if let Some((lease, handle)) = owned.remove(&partition_id) {
                                    handle.abort();
                                    if let Err(e) = coordinator.release(&lease).await {
                                        tracing::warn!(
                                            owner = %owner_id,
                                            partition = partition_id,
                                            "coordinated reader: surplus release failed: {e}"
                                        );
                                    }
                                }
                            }
                        }

                        next_rebalance = tokio::time::Instant::now()
                            + jittered_duration(rebalance_interval);
                    }

                    _ = tokio::time::sleep_until(next_renew) => {
                        let partition_ids: Vec<u16> = owned.keys().copied().collect();
                        let mut lost: Vec<u16> = Vec::new();
                        for partition_id in partition_ids {
                            if let Some((lease, _)) = owned.get(&partition_id) {
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
                        for partition_id in lost {
                            tracing::info!(
                                owner = %owner_id,
                                partition = partition_id,
                                "coordinated reader: partition ownership lost, stopping worker"
                            );
                            if let Some((_, handle)) = owned.remove(&partition_id) {
                                handle.abort();
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

                        next_renew =
                            tokio::time::Instant::now() + jittered_duration(tick_duration);
                    }

                    _ = shutdown_notify.notified() => {
                        for (partition_id, (lease, handle)) in owned.drain() {
                            handle.abort();
                            if let Err(e) = coordinator.release(&lease).await {
                                tracing::warn!(
                                    owner = %owner_id,
                                    partition = partition_id,
                                    "coordinated reader: release on shutdown failed: {e}"
                                );
                            }
                        }
                        return;
                    }
                }

                if tx.is_closed() {
                    for (partition_id, (lease, handle)) in owned.drain() {
                        handle.abort();
                        if let Err(e) = coordinator.release(&lease).await {
                            tracing::warn!(
                                owner = %owner_id,
                                partition = partition_id,
                                "coordinated reader: release on stream close failed: {e}"
                            );
                        }
                    }
                    return;
                }
            }
        });

        Ok(CoordinatedStream { rx, shutdown })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_partition_count_single_consumer() {
        let total = NonZeroU16::new(8).unwrap();
        assert_eq!(target_partition_count(total, 1, 0), 8);
    }

    #[test]
    fn target_partition_count_two_consumers() {
        let total = NonZeroU16::new(8).unwrap();
        assert_eq!(target_partition_count(total, 2, 0), 4);
    }

    #[test]
    fn target_partition_count_three_consumers_no_slack() {
        let total = NonZeroU16::new(8).unwrap();
        assert_eq!(target_partition_count(total, 3, 0), 3);
    }

    #[test]
    fn target_partition_count_three_consumers_with_slack() {
        let total = NonZeroU16::new(8).unwrap();
        assert_eq!(target_partition_count(total, 3, 1), 4);
    }

    #[test]
    fn target_partition_count_zero_consumers_treated_as_one() {
        let total = NonZeroU16::new(8).unwrap();
        assert_eq!(target_partition_count(total, 0, 0), 8);
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
}
