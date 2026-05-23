use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use futures::stream::select_all;
use tokio::sync::mpsc;

use eventuary_core::io::stream::SpawnedStream;
use eventuary_core::io::{
    ConsumerGroupId, Message, OwnerId, PartitionCoordinator, PartitionLease, Reader, StartFrom,
    StreamId,
};
use eventuary_core::{Error, Result};

use crate::coordinated_acker::PgCoordinatedAcker;
use crate::partition_coordinator::PgPartitionCoordinator;
use crate::partition_cursor::PgPartitionCursor;
use crate::reader::{PgCursor, PgPartitionSelection, PgReader, PgSubscription};

#[derive(Clone, Copy)]
pub struct PgCoordinatedReaderConfig {
    pub partition_lease_duration: Duration,
    pub partition_renew_interval: Duration,
    pub consumer_lease_duration: Duration,
    pub consumer_heartbeat_interval: Duration,
}

impl Default for PgCoordinatedReaderConfig {
    fn default() -> Self {
        Self {
            partition_lease_duration: Duration::from_secs(60),
            partition_renew_interval: Duration::from_secs(15),
            consumer_lease_duration: Duration::from_secs(30),
            consumer_heartbeat_interval: Duration::from_secs(10),
        }
    }
}

#[derive(Clone)]
pub struct PgCoordinatedSubscription {
    pub consumer_group_id: ConsumerGroupId,
    pub stream_id: StreamId,
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

impl Reader for PgCoordinatedReader {
    type Subscription = PgCoordinatedSubscription;
    type Acker = PgCoordinatedAcker;
    type Cursor = PgPartitionCursor;
    type Stream = SpawnedStream<PgCoordinatedAcker, PgPartitionCursor>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        self.coordinator
            .heartbeat(
                &subscription.consumer_group_id,
                &subscription.stream_id,
                &self.owner_id,
                self.config.consumer_lease_duration,
            )
            .await?;

        let mut leases: Vec<PartitionLease> = Vec::new();
        for partition_id in 0..subscription.partition_count.get() {
            if let Some(lease) = self
                .coordinator
                .claim(
                    &subscription.consumer_group_id,
                    &subscription.stream_id,
                    partition_id,
                    &self.owner_id,
                    self.config.partition_lease_duration,
                )
                .await?
            {
                leases.push(lease);
            }
        }

        let (tx, rx) = mpsc::channel::<Result<Message<PgCoordinatedAcker, PgPartitionCursor>>>(16);

        let inner_reader = self.inner.clone();
        let coordinator = Arc::clone(&self.coordinator);
        let owner_id = self.owner_id.clone();
        let partition_count = subscription.partition_count;
        let group = subscription.consumer_group_id.clone();
        let stream_id_val = subscription.stream_id.clone();
        let base_sub = subscription.inner.clone();
        let start = subscription.start.clone();
        let partition_lease_duration = self.config.partition_lease_duration;
        let consumer_lease_duration = self.config.consumer_lease_duration;
        let renew_interval = self.config.partition_renew_interval;
        let heartbeat_interval = self.config.consumer_heartbeat_interval;

        let handle = tokio::spawn(async move {
            let mut tagged_streams = Vec::new();

            for lease in &leases {
                let inner_start = if lease.checkpoint_sequence == 0 {
                    start.clone()
                } else {
                    StartFrom::After(PgCursor::new(lease.checkpoint_sequence))
                };
                let inner_sub = PgSubscription {
                    start: inner_start,
                    partitions: PgPartitionSelection::One {
                        count: partition_count,
                        id: lease.partition_id,
                    },
                    ..base_sub.clone()
                };

                let lease_clone = lease.clone();
                match inner_reader.read(inner_sub).await {
                    Ok(stream) => {
                        let tagged = stream.map(move |item| (lease_clone.clone(), item));
                        tagged_streams.push(tagged);
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                }
            }

            let mut merged = select_all(tagged_streams);
            let tick_duration = renew_interval.min(heartbeat_interval);
            let mut interval = tokio::time::interval(tick_duration);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        for lease in &leases {
                            match coordinator
                                .renew(
                                    &group,
                                    &stream_id_val,
                                    lease.partition_id,
                                    &owner_id,
                                    lease.generation,
                                    partition_lease_duration,
                                )
                                .await
                            {
                                Ok(()) => {}
                                Err(Error::OwnershipLost(_)) => {}
                                Err(_) => {}
                            }
                        }
                        let _ = coordinator
                            .heartbeat(&group, &stream_id_val, &owner_id, consumer_lease_duration)
                            .await;
                    }
                    item = merged.next() => {
                        match item {
                            None => return,
                            Some((lease, Ok(msg))) => {
                                let (event, inner_acker, inner_cursor) = msg.into_parts();
                                let coordinated_acker = PgCoordinatedAcker::new(
                                    inner_acker,
                                    Arc::clone(&coordinator),
                                    lease.consumer_group_id.clone(),
                                    lease.stream_id.clone(),
                                    lease.partition_id,
                                    owner_id.clone(),
                                    lease.generation,
                                    inner_cursor.sequence,
                                );
                                let coordinated_cursor = PgPartitionCursor::new(
                                    partition_count,
                                    lease.partition_id,
                                    inner_cursor.sequence,
                                );
                                let out = Message::new(event, coordinated_acker, coordinated_cursor);
                                if tx.send(Ok(out)).await.is_err() {
                                    return;
                                }
                            }
                            Some((_, Err(e))) => {
                                let _ = tx.send(Err(e)).await;
                                return;
                            }
                        }
                    }
                }
            }
        });

        Ok(SpawnedStream::new(rx, handle))
    }
}
