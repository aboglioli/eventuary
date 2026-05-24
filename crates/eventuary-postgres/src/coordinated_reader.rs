use std::collections::HashMap;
use std::num::NonZeroU16;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Stream;
use tokio::sync::{Notify, mpsc};

use eventuary_core::Partition;
use eventuary_core::io::reader::CheckpointScope;
use eventuary_core::io::{
    Message, OwnerId, PartitionCoordinator, PartitionLease, Reader, StartFrom,
};
use eventuary_core::partition::PartitionSelection;
use eventuary_core::{Error, Result};

use crate::coordinated_acker::PgCoordinatedAcker;
use crate::partition_coordinator::PgPartitionCoordinator;
use crate::partition_cursor::PgPartitionCursor;
use crate::reader::{PgCursor, PgReader, PgSubscription};

#[derive(Clone, Copy)]
pub struct PgCoordinatedReaderConfig {
    pub partition_lease_duration: Duration,
    pub partition_renew_interval: Duration,
    pub consumer_lease_duration: Duration,
    pub consumer_heartbeat_interval: Duration,
    pub rebalance_interval: Duration,
    pub partition_slack: u16,
}

impl Default for PgCoordinatedReaderConfig {
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

#[derive(Clone)]
pub struct PgCoordinatedSubscription {
    pub scope: CheckpointScope,
    pub partition_count: NonZeroU16,
    pub start: StartFrom<PgCursor>,
    pub inner: PgSubscription,
}

#[derive(Clone)]
pub struct PgCoordinatedReader {
    inner: PgReader,
    coordinator: Arc<PgPartitionCoordinator>,
    owner_id: OwnerId,
    config: PgCoordinatedReaderConfig,
}

impl PgCoordinatedReader {
    pub fn new(
        inner: PgReader,
        coordinator: Arc<PgPartitionCoordinator>,
        owner_id: OwnerId,
        config: PgCoordinatedReaderConfig,
    ) -> Self {
        Self {
            inner,
            coordinator,
            owner_id,
            config,
        }
    }
}

pub struct PgCoordinatedStream {
    rx: mpsc::Receiver<Result<Message<PgCoordinatedAcker, PgPartitionCursor>>>,
    shutdown: Arc<Notify>,
}

impl Stream for PgCoordinatedStream {
    type Item = Result<Message<PgCoordinatedAcker, PgPartitionCursor>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

impl Drop for PgCoordinatedStream {
    fn drop(&mut self) {
        self.shutdown.notify_one();
    }
}

fn jittered_duration(base: Duration) -> Duration {
    use rand::Rng;
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

async fn partition_worker(
    inner_reader: PgReader,
    coordinator: Arc<PgPartitionCoordinator>,
    partition_count: NonZeroU16,
    lease: PartitionLease<PgCursor>,
    inner_subscription_template: PgSubscription,
    start: StartFrom<PgCursor>,
    tx: mpsc::Sender<Result<Message<PgCoordinatedAcker, PgPartitionCursor>>>,
) {
    use futures::StreamExt;

    let checkpoint_seq = lease
        .checkpoint_cursor
        .as_ref()
        .map(|c| c.sequence)
        .unwrap_or(0);
    let inner_start = if checkpoint_seq == 0 {
        start
    } else {
        StartFrom::After(PgCursor::new(checkpoint_seq))
    };
    let inner_sub = PgSubscription {
        start: inner_start,
        partitions: PartitionSelection::One(lease.partition),
        ..inner_subscription_template
    };

    let mut inner_stream = match inner_reader.read(inner_sub).await {
        Ok(s) => s,
        Err(e) => {
            let _ = tx.send(Err(e)).await;
            return;
        }
    };

    while let Some(item) = inner_stream.next().await {
        match item {
            Ok(msg) => {
                let (event, inner_acker, inner_cursor) = msg.into_parts();
                let coordinated_acker = PgCoordinatedAcker::new(
                    inner_acker,
                    Arc::clone(&coordinator),
                    lease.clone(),
                    inner_cursor.sequence,
                );
                let coordinated_cursor = PgPartitionCursor::new(
                    partition_count,
                    lease.partition.id(),
                    inner_cursor.sequence,
                );
                let out = Message::new(event, coordinated_acker, coordinated_cursor);
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

impl Reader for PgCoordinatedReader {
    type Subscription = PgCoordinatedSubscription;
    type Acker = PgCoordinatedAcker;
    type Cursor = PgPartitionCursor;
    type Stream = PgCoordinatedStream;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        self.coordinator
            .heartbeat(
                &subscription.scope,
                &self.owner_id,
                self.config.consumer_lease_duration,
            )
            .await?;

        let (tx, rx) = mpsc::channel::<Result<Message<PgCoordinatedAcker, PgPartitionCursor>>>(16);
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
            let mut owned: HashMap<u16, (PartitionLease<PgCursor>, tokio::task::JoinHandle<()>)> =
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
                                let _ = coordinator.release(&lease).await;
                            }
                        }

                        let live = match coordinator.live_consumers(&scope).await {
                            Ok(n) => n,
                            Err(_) => owned.len().max(1),
                        };
                        let target = target_partition_count(partition_count, live, slack) as usize;

                        if owned.len() < target {
                            for partition_id in 0..partition_count.get() {
                                if owned.len() >= target {
                                    break;
                                }
                                if owned.contains_key(&partition_id) {
                                    continue;
                                }
                                let partition = match Partition::new(partition_id, partition_count) {
                                    Ok(p) => p,
                                    Err(_) => continue,
                                };
                                match coordinator
                                    .claim(&scope, &owner_id, partition, partition_lease_duration)
                                    .await
                                {
                                    Ok(Some(lease)) => {
                                        let worker_handle = tokio::spawn(partition_worker(
                                            inner_reader.clone(),
                                            Arc::clone(&coordinator),
                                            partition_count,
                                            lease.clone(),
                                            base_sub.clone(),
                                            start.clone(),
                                            tx.clone(),
                                        ));
                                        owned.insert(partition_id, (lease, worker_handle));
                                    }
                                    Ok(None) => {}
                                    Err(_) => {}
                                }
                            }
                        } else if owned.len() > target {
                            let mut to_release: Vec<u16> = owned.keys().copied().collect();
                            to_release.sort_by(|a, b| b.cmp(a));
                            let surplus = owned.len() - target;
                            for partition_id in to_release.into_iter().take(surplus) {
                                if let Some((lease, handle)) = owned.remove(&partition_id) {
                                    handle.abort();
                                    let _ = coordinator.release(&lease).await;
                                }
                            }
                        }

                        next_rebalance =
                            tokio::time::Instant::now() + jittered_duration(rebalance_interval);
                    }

                    _ = tokio::time::sleep_until(next_renew) => {
                        let partition_ids: Vec<u16> = owned.keys().copied().collect();
                        let mut lost: Vec<u16> = Vec::new();
                        for partition_id in partition_ids {
                            if let Some((lease, _)) = owned.get(&partition_id) {
                                match coordinator.renew(lease, partition_lease_duration).await {
                                    Ok(()) => {}
                                    Err(Error::OwnershipLost(_)) => {
                                        lost.push(partition_id);
                                    }
                                    Err(_) => {}
                                }
                            }
                        }
                        for partition_id in lost {
                            if let Some((_, handle)) = owned.remove(&partition_id) {
                                handle.abort();
                            }
                        }
                        let _ = coordinator
                            .heartbeat(&scope, &owner_id, consumer_lease_duration)
                            .await;

                        next_renew =
                            tokio::time::Instant::now() + jittered_duration(tick_duration);
                    }

                    _ = shutdown_notify.notified() => {
                        for (_, (lease, handle)) in owned.drain() {
                            handle.abort();
                            let _ = coordinator.release(&lease).await;
                        }
                        return;
                    }
                }

                if tx.is_closed() {
                    for (_, (lease, handle)) in owned.drain() {
                        handle.abort();
                        let _ = coordinator.release(&lease).await;
                    }
                    return;
                }
            }
        });

        Ok(PgCoordinatedStream { rx, shutdown })
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
