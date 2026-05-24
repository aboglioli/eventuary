use std::sync::Arc;

use eventuary_core::Result;
use eventuary_core::io::{Acker, PartitionCoordinator, PartitionLease};

use crate::partition_coordinator::PgPartitionCoordinator;
use crate::reader::{PgCursor, PgCursorAcker};

pub struct PgCoordinatedAcker {
    inner: PgCursorAcker,
    coordinator: Arc<PgPartitionCoordinator>,
    lease: PartitionLease<PgCursor>,
    sequence: i64,
}

impl PgCoordinatedAcker {
    pub fn new(
        inner: PgCursorAcker,
        coordinator: Arc<PgPartitionCoordinator>,
        lease: PartitionLease<PgCursor>,
        sequence: i64,
    ) -> Self {
        Self {
            inner,
            coordinator,
            lease,
            sequence,
        }
    }
}

impl Acker for PgCoordinatedAcker {
    async fn ack(&self) -> Result<()> {
        self.inner.ack().await?;
        self.coordinator
            .checkpoint(&self.lease, PgCursor::new(self.sequence))
            .await
    }

    async fn nack(&self) -> Result<()> {
        self.inner.nack().await
    }
}
