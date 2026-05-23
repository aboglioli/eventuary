use std::sync::Arc;

use eventuary_core::Result;
use eventuary_core::io::{Acker, ConsumerGroupId, OwnerId, PartitionCoordinator, StreamId};

use crate::partition_coordinator::PgPartitionCoordinator;
use crate::reader::PgCursorAcker;

pub struct PgCoordinatedAcker {
    inner: PgCursorAcker,
    coordinator: Arc<PgPartitionCoordinator>,
    consumer_group_id: ConsumerGroupId,
    stream_id: StreamId,
    partition_id: u16,
    owner_id: OwnerId,
    generation: i64,
    sequence: i64,
}

impl PgCoordinatedAcker {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        inner: PgCursorAcker,
        coordinator: Arc<PgPartitionCoordinator>,
        consumer_group_id: ConsumerGroupId,
        stream_id: StreamId,
        partition_id: u16,
        owner_id: OwnerId,
        generation: i64,
        sequence: i64,
    ) -> Self {
        Self {
            inner,
            coordinator,
            consumer_group_id,
            stream_id,
            partition_id,
            owner_id,
            generation,
            sequence,
        }
    }
}

impl Acker for PgCoordinatedAcker {
    async fn ack(&self) -> Result<()> {
        self.inner.ack().await?;
        self.coordinator
            .checkpoint(
                &self.consumer_group_id,
                &self.stream_id,
                self.partition_id,
                &self.owner_id,
                self.generation,
                self.sequence,
            )
            .await
    }

    async fn nack(&self) -> Result<()> {
        self.inner.nack().await
    }
}
