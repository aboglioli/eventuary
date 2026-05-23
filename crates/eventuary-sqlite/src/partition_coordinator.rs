//! SQLite partition coordinator.
//!
//! Suitable for single-host or few-process coordination scenarios. Multi-host
//! production deployments should use the Postgres coordinator instead — SQLite
//! write locking will serialize claims, renewals, and checkpoints across
//! processes.

use std::sync::Arc;

use chrono::{Duration, Utc};

use eventuary_core::io::{
    ConsumerGroupId, OwnerId, PartitionCoordinator, PartitionLease, StreamId,
};
use eventuary_core::{Error, Result};

use crate::database::SqliteConn;
use crate::relation::SqliteRelationName;

#[derive(Debug, Clone)]
pub struct SqlitePartitionCoordinatorConfig {
    pub consumers_relation: SqliteRelationName,
    pub partitions_relation: SqliteRelationName,
}

impl Default for SqlitePartitionCoordinatorConfig {
    fn default() -> Self {
        Self {
            consumers_relation: SqliteRelationName::new("event_stream_consumers")
                .expect("default consumers relation"),
            partitions_relation: SqliteRelationName::new("event_stream_partitions")
                .expect("default partitions relation"),
        }
    }
}

pub struct SqlitePartitionCoordinator {
    conn: SqliteConn,
    consumers_relation: Arc<String>,
    partitions_relation: Arc<String>,
}

impl Clone for SqlitePartitionCoordinator {
    fn clone(&self) -> Self {
        Self {
            conn: Arc::clone(&self.conn),
            consumers_relation: Arc::clone(&self.consumers_relation),
            partitions_relation: Arc::clone(&self.partitions_relation),
        }
    }
}

impl SqlitePartitionCoordinator {
    pub fn new(conn: SqliteConn, config: SqlitePartitionCoordinatorConfig) -> Self {
        Self {
            conn,
            consumers_relation: Arc::new(config.consumers_relation.render()),
            partitions_relation: Arc::new(config.partitions_relation.render()),
        }
    }
}

fn compute_lease_until(lease_duration: std::time::Duration) -> Result<String> {
    let dt = Utc::now()
        + Duration::from_std(lease_duration)
            .map_err(|_| Error::Config("lease duration out of range".to_owned()))?;
    Ok(dt.to_rfc3339())
}

impl PartitionCoordinator for SqlitePartitionCoordinator {
    async fn heartbeat<'a>(
        &'a self,
        consumer_group_id: &'a ConsumerGroupId,
        stream_id: &'a StreamId,
        owner_id: &'a OwnerId,
        lease_duration: std::time::Duration,
    ) -> Result<()> {
        let lease_until = compute_lease_until(lease_duration)?;
        let conn = Arc::clone(&self.conn);
        let consumers = Arc::clone(&self.consumers_relation);
        let group = consumer_group_id.as_str().to_owned();
        let stream = stream_id.as_str().to_owned();
        let owner = owner_id.as_str().to_owned();
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            let sql = format!(
                "INSERT INTO {consumers} (consumer_group_id, stream_id, owner_id, lease_until) \
                 VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT (consumer_group_id, stream_id, owner_id) \
                 DO UPDATE SET lease_until = excluded.lease_until"
            );
            guard
                .execute(&sql, rusqlite::params![group, stream, owner, lease_until])
                .map_err(|e| Error::Store(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
    }

    async fn live_consumers<'a>(
        &'a self,
        consumer_group_id: &'a ConsumerGroupId,
        stream_id: &'a StreamId,
    ) -> Result<usize> {
        let conn = Arc::clone(&self.conn);
        let consumers = Arc::clone(&self.consumers_relation);
        let group = consumer_group_id.as_str().to_owned();
        let stream = stream_id.as_str().to_owned();
        let now = Utc::now().to_rfc3339();
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            let sql = format!(
                "SELECT COUNT(*) FROM {consumers} \
                 WHERE consumer_group_id = ?1 \
                   AND stream_id = ?2 \
                   AND lease_until > ?3"
            );
            let count: i64 = guard
                .query_row(&sql, rusqlite::params![group, stream, now], |r| r.get(0))
                .map_err(|e| Error::Store(e.to_string()))?;
            Ok(count as usize)
        })
        .await
        .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
    }

    async fn claim<'a>(
        &'a self,
        consumer_group_id: &'a ConsumerGroupId,
        stream_id: &'a StreamId,
        partition_id: u16,
        owner_id: &'a OwnerId,
        lease_duration: std::time::Duration,
    ) -> Result<Option<PartitionLease>> {
        let lease_until = compute_lease_until(lease_duration)?;
        let conn = Arc::clone(&self.conn);
        let partitions = Arc::clone(&self.partitions_relation);
        let group = consumer_group_id.as_str().to_owned();
        let stream = stream_id.as_str().to_owned();
        let owner = owner_id.as_str().to_owned();
        let partition_id_i64 = partition_id as i64;
        let now = Utc::now().to_rfc3339();
        let consumer_group_id = consumer_group_id.clone();
        let stream_id = stream_id.clone();
        let owner_id = owner_id.clone();
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            let sql = format!(
                "INSERT INTO {partitions} \
                   (consumer_group_id, stream_id, partition_id, owner_id, lease_until, generation, checkpoint_sequence) \
                 VALUES (?1, ?2, ?3, ?4, ?5, 1, 0) \
                 ON CONFLICT (consumer_group_id, stream_id, partition_id) DO UPDATE \
                 SET owner_id = excluded.owner_id, \
                     lease_until = excluded.lease_until, \
                     generation = {partitions}.generation + 1 \
                 WHERE {partitions}.owner_id IS NULL \
                    OR {partitions}.lease_until IS NULL \
                    OR {partitions}.lease_until < ?6 \
                    OR {partitions}.owner_id = excluded.owner_id \
                 RETURNING owner_id, lease_until, generation, checkpoint_sequence"
            );
            let row = guard
                .query_row(
                    &sql,
                    rusqlite::params![group, stream, partition_id_i64, owner, lease_until, now],
                    |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, i64>(2)?,
                            r.get::<_, i64>(3)?,
                        ))
                    },
                )
                .map(Some)
                .or_else(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => Ok(None),
                    other => Err(other),
                })
                .map_err(|e| Error::Store(e.to_string()))?;
            match row {
                None => Ok(None),
                Some((_returned_owner, lease_until_text, generation, checkpoint_sequence)) => {
                    let lease_until = chrono::DateTime::parse_from_rfc3339(&lease_until_text)
                        .map(|dt| dt.with_timezone(&Utc))
                        .map_err(|e| {
                            Error::Serialization(format!("lease_until decode: {e}"))
                        })?;
                    Ok(Some(PartitionLease {
                        consumer_group_id,
                        stream_id,
                        partition_id,
                        owner_id,
                        generation,
                        checkpoint_sequence,
                        lease_until,
                    }))
                }
            }
        })
        .await
        .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
    }

    async fn renew<'a>(
        &'a self,
        consumer_group_id: &'a ConsumerGroupId,
        stream_id: &'a StreamId,
        partition_id: u16,
        owner_id: &'a OwnerId,
        generation: i64,
        lease_duration: std::time::Duration,
    ) -> Result<()> {
        let lease_until = compute_lease_until(lease_duration)?;
        let conn = Arc::clone(&self.conn);
        let partitions = Arc::clone(&self.partitions_relation);
        let group = consumer_group_id.as_str().to_owned();
        let stream = stream_id.as_str().to_owned();
        let owner = owner_id.as_str().to_owned();
        let partition_id_i64 = partition_id as i64;
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            let sql = format!(
                "UPDATE {partitions} \
                 SET lease_until = ?5 \
                 WHERE consumer_group_id = ?1 \
                   AND stream_id = ?2 \
                   AND partition_id = ?3 \
                   AND owner_id = ?4 \
                   AND generation = ?6"
            );
            let affected = guard
                .execute(
                    &sql,
                    rusqlite::params![
                        group,
                        stream,
                        partition_id_i64,
                        owner,
                        lease_until,
                        generation
                    ],
                )
                .map_err(|e| Error::Store(e.to_string()))?;
            if affected == 0 {
                return Err(Error::OwnershipLost(format!(
                    "partition {partition_id} generation {generation}"
                )));
            }
            Ok(())
        })
        .await
        .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
    }

    async fn release<'a>(
        &'a self,
        consumer_group_id: &'a ConsumerGroupId,
        stream_id: &'a StreamId,
        partition_id: u16,
        owner_id: &'a OwnerId,
        generation: i64,
    ) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        let partitions = Arc::clone(&self.partitions_relation);
        let group = consumer_group_id.as_str().to_owned();
        let stream = stream_id.as_str().to_owned();
        let owner = owner_id.as_str().to_owned();
        let partition_id_i64 = partition_id as i64;
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            let sql = format!(
                "UPDATE {partitions} \
                 SET owner_id = NULL, \
                     lease_until = NULL, \
                     generation = generation + 1 \
                 WHERE consumer_group_id = ?1 \
                   AND stream_id = ?2 \
                   AND partition_id = ?3 \
                   AND owner_id = ?4 \
                   AND generation = ?5"
            );
            let affected = guard
                .execute(
                    &sql,
                    rusqlite::params![group, stream, partition_id_i64, owner, generation],
                )
                .map_err(|e| Error::Store(e.to_string()))?;
            if affected == 0 {
                return Err(Error::OwnershipLost(format!(
                    "partition {partition_id} generation {generation}"
                )));
            }
            Ok(())
        })
        .await
        .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
    }

    async fn checkpoint<'a>(
        &'a self,
        consumer_group_id: &'a ConsumerGroupId,
        stream_id: &'a StreamId,
        partition_id: u16,
        owner_id: &'a OwnerId,
        generation: i64,
        sequence: i64,
    ) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        let partitions = Arc::clone(&self.partitions_relation);
        let group = consumer_group_id.as_str().to_owned();
        let stream = stream_id.as_str().to_owned();
        let owner = owner_id.as_str().to_owned();
        let partition_id_i64 = partition_id as i64;
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            let sql = format!(
                "UPDATE {partitions} \
                 SET checkpoint_sequence = ?6 \
                 WHERE consumer_group_id = ?1 \
                   AND stream_id = ?2 \
                   AND partition_id = ?3 \
                   AND owner_id = ?4 \
                   AND generation = ?5 \
                   AND ?6 > {partitions}.checkpoint_sequence"
            );
            let affected = guard
                .execute(
                    &sql,
                    rusqlite::params![group, stream, partition_id_i64, owner, generation, sequence],
                )
                .map_err(|e| Error::Store(e.to_string()))?;
            if affected == 0 {
                let check_sql = format!(
                    "SELECT owner_id, generation FROM {partitions} \
                     WHERE consumer_group_id = ?1 AND stream_id = ?2 AND partition_id = ?3"
                );
                let check_row = guard
                    .query_row(
                        &check_sql,
                        rusqlite::params![group, stream, partition_id_i64],
                        |r| {
                            Ok((
                                r.get::<_, Option<String>>(0)?,
                                r.get::<_, i64>(1)?,
                            ))
                        },
                    )
                    .map(Some)
                    .or_else(|e| match e {
                        rusqlite::Error::QueryReturnedNoRows => Ok(None),
                        other => Err(other),
                    })
                    .map_err(|e| Error::Store(e.to_string()))?;
                match check_row {
                    Some((current_owner, current_generation)) => {
                        if current_generation == generation
                            && current_owner.as_deref() == Some(owner.as_str())
                        {
                            return Ok(());
                        }
                        Err(Error::OwnershipLost(format!(
                            "checkpoint rejected for partition {partition_id}: stale owner/generation"
                        )))
                    }
                    None => Err(Error::OwnershipLost(format!(
                        "partition {partition_id} generation {generation}"
                    ))),
                }
            } else {
                Ok(())
            }
        })
        .await
        .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventuary_core::io::{ConsumerGroupId, OwnerId, PartitionCoordinator, StreamId};

    use crate::database::SqliteDatabase;

    fn make_coordinator() -> SqlitePartitionCoordinator {
        let db = SqliteDatabase::open_in_memory().unwrap();
        SqlitePartitionCoordinator::new(db.conn(), SqlitePartitionCoordinatorConfig::default())
    }

    fn group() -> ConsumerGroupId {
        ConsumerGroupId::new("group-1").unwrap()
    }

    fn stream() -> StreamId {
        StreamId::new("orders").unwrap()
    }

    #[tokio::test]
    async fn heartbeat_and_live_count() {
        let coord = make_coordinator();
        let g = group();
        let s = stream();
        let owner_a = OwnerId::new("worker-a").unwrap();
        let owner_b = OwnerId::new("worker-b").unwrap();
        let long_lease = std::time::Duration::from_secs(60);

        coord.heartbeat(&g, &s, &owner_a, long_lease).await.unwrap();
        coord.heartbeat(&g, &s, &owner_b, long_lease).await.unwrap();

        let live = coord.live_consumers(&g, &s).await.unwrap();
        assert_eq!(live, 2);
    }

    #[tokio::test]
    async fn live_consumers_excludes_expired() {
        let coord = make_coordinator();
        let g = group();
        let s = stream();
        let owner_a = OwnerId::new("worker-a").unwrap();
        let owner_b = OwnerId::new("worker-b").unwrap();
        let long_lease = std::time::Duration::from_secs(60);
        let short_lease = std::time::Duration::from_millis(50);

        coord.heartbeat(&g, &s, &owner_a, long_lease).await.unwrap();
        coord
            .heartbeat(&g, &s, &owner_b, short_lease)
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let live = coord.live_consumers(&g, &s).await.unwrap();
        assert_eq!(live, 1);
    }

    #[tokio::test]
    async fn claim_free_partition_succeeds() {
        let coord = make_coordinator();
        let g = group();
        let s = stream();
        let owner = OwnerId::new("worker-a").unwrap();
        let lease_dur = std::time::Duration::from_secs(60);

        let lease = coord
            .claim(&g, &s, 0, &owner, lease_dur)
            .await
            .unwrap()
            .expect("should get lease");

        assert_eq!(lease.generation, 1);
        assert_eq!(lease.checkpoint_sequence, 0);
    }

    #[tokio::test]
    async fn claim_contested_returns_none() {
        let coord = make_coordinator();
        let g = group();
        let s = stream();
        let owner_a = OwnerId::new("worker-a").unwrap();
        let owner_b = OwnerId::new("worker-b").unwrap();
        let lease_dur = std::time::Duration::from_secs(60);

        let first = coord.claim(&g, &s, 0, &owner_a, lease_dur).await.unwrap();
        assert!(first.is_some());

        let second = coord.claim(&g, &s, 0, &owner_b, lease_dur).await.unwrap();
        assert!(second.is_none());
    }

    #[tokio::test]
    async fn claim_after_expiry_succeeds() {
        let coord = make_coordinator();
        let g = group();
        let s = stream();
        let owner_a = OwnerId::new("worker-a").unwrap();
        let owner_b = OwnerId::new("worker-b").unwrap();
        let short_lease = std::time::Duration::from_millis(50);
        let long_lease = std::time::Duration::from_secs(60);

        let first = coord.claim(&g, &s, 0, &owner_a, short_lease).await.unwrap();
        assert_eq!(first.unwrap().generation, 1);

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let second = coord
            .claim(&g, &s, 0, &owner_b, long_lease)
            .await
            .unwrap()
            .expect("should succeed after expiry");
        assert_eq!(second.generation, 2);
    }

    #[tokio::test]
    async fn renew_with_matching_generation() {
        let coord = make_coordinator();
        let g = group();
        let s = stream();
        let owner = OwnerId::new("worker-a").unwrap();
        let lease_dur = std::time::Duration::from_secs(60);

        let lease = coord
            .claim(&g, &s, 0, &owner, lease_dur)
            .await
            .unwrap()
            .unwrap();

        coord
            .renew(&g, &s, 0, &owner, lease.generation, lease_dur)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn renew_with_stale_generation_returns_ownership_lost() {
        let coord = make_coordinator();
        let g = group();
        let s = stream();
        let owner_a = OwnerId::new("worker-a").unwrap();
        let owner_b = OwnerId::new("worker-b").unwrap();
        let short_lease = std::time::Duration::from_millis(50);
        let long_lease = std::time::Duration::from_secs(60);

        let lease_a = coord
            .claim(&g, &s, 0, &owner_a, short_lease)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lease_a.generation, 1);

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        coord
            .claim(&g, &s, 0, &owner_b, long_lease)
            .await
            .unwrap()
            .unwrap();

        let err = coord
            .renew(&g, &s, 0, &owner_a, lease_a.generation, long_lease)
            .await
            .unwrap_err();
        assert!(matches!(err, eventuary_core::Error::OwnershipLost(_)));
    }

    #[tokio::test]
    async fn release_with_matching_generation() {
        let coord = make_coordinator();
        let g = group();
        let s = stream();
        let owner_a = OwnerId::new("worker-a").unwrap();
        let owner_b = OwnerId::new("worker-b").unwrap();
        let lease_dur = std::time::Duration::from_secs(60);

        let lease = coord
            .claim(&g, &s, 0, &owner_a, lease_dur)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lease.generation, 1);

        coord
            .release(&g, &s, 0, &owner_a, lease.generation)
            .await
            .unwrap();

        let new_lease = coord
            .claim(&g, &s, 0, &owner_b, lease_dur)
            .await
            .unwrap()
            .expect("should claim after release");
        assert_eq!(new_lease.generation, 3);
    }

    #[tokio::test]
    async fn release_with_stale_generation_returns_ownership_lost() {
        let coord = make_coordinator();
        let g = group();
        let s = stream();
        let owner_a = OwnerId::new("worker-a").unwrap();
        let owner_b = OwnerId::new("worker-b").unwrap();
        let short_lease = std::time::Duration::from_millis(50);
        let long_lease = std::time::Duration::from_secs(60);

        let lease_a = coord
            .claim(&g, &s, 0, &owner_a, short_lease)
            .await
            .unwrap()
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        coord
            .claim(&g, &s, 0, &owner_b, long_lease)
            .await
            .unwrap()
            .unwrap();

        let err = coord
            .release(&g, &s, 0, &owner_a, lease_a.generation)
            .await
            .unwrap_err();
        assert!(matches!(err, eventuary_core::Error::OwnershipLost(_)));
    }

    #[tokio::test]
    async fn checkpoint_with_matching_generation_advances() {
        let coord = make_coordinator();
        let g = group();
        let s = stream();
        let owner_a = OwnerId::new("worker-a").unwrap();
        let owner_b = OwnerId::new("worker-b").unwrap();
        let long_lease = std::time::Duration::from_secs(60);

        let lease = coord
            .claim(&g, &s, 0, &owner_a, long_lease)
            .await
            .unwrap()
            .unwrap();

        coord
            .checkpoint(&g, &s, 0, &owner_a, lease.generation, 100)
            .await
            .unwrap();

        coord
            .release(&g, &s, 0, &owner_a, lease.generation)
            .await
            .unwrap();

        let new_lease = coord
            .claim(&g, &s, 0, &owner_b, long_lease)
            .await
            .unwrap()
            .expect("should claim after release");
        assert_eq!(new_lease.checkpoint_sequence, 100);
    }

    #[tokio::test]
    async fn checkpoint_is_monotonic() {
        let coord = make_coordinator();
        let g = group();
        let s = stream();
        let owner_a = OwnerId::new("worker-a").unwrap();
        let owner_b = OwnerId::new("worker-b").unwrap();
        let long_lease = std::time::Duration::from_secs(60);

        let lease = coord
            .claim(&g, &s, 0, &owner_a, long_lease)
            .await
            .unwrap()
            .unwrap();

        coord
            .checkpoint(&g, &s, 0, &owner_a, lease.generation, 100)
            .await
            .unwrap();

        coord
            .checkpoint(&g, &s, 0, &owner_a, lease.generation, 50)
            .await
            .unwrap();

        coord
            .release(&g, &s, 0, &owner_a, lease.generation)
            .await
            .unwrap();

        let new_lease = coord
            .claim(&g, &s, 0, &owner_b, long_lease)
            .await
            .unwrap()
            .expect("should claim after release");
        assert_eq!(new_lease.checkpoint_sequence, 100);
    }

    #[tokio::test]
    async fn checkpoint_with_stale_generation_returns_ownership_lost() {
        let coord = make_coordinator();
        let g = group();
        let s = stream();
        let owner_a = OwnerId::new("worker-a").unwrap();
        let owner_b = OwnerId::new("worker-b").unwrap();
        let short_lease = std::time::Duration::from_millis(50);
        let long_lease = std::time::Duration::from_secs(60);

        let lease_a = coord
            .claim(&g, &s, 0, &owner_a, short_lease)
            .await
            .unwrap()
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        coord
            .claim(&g, &s, 0, &owner_b, long_lease)
            .await
            .unwrap()
            .unwrap();

        let err = coord
            .checkpoint(&g, &s, 0, &owner_a, lease_a.generation, 100)
            .await
            .unwrap_err();
        assert!(matches!(err, eventuary_core::Error::OwnershipLost(_)));
    }

    #[tokio::test]
    async fn checkpoint_after_release_returns_ownership_lost() {
        let coord = make_coordinator();
        let g = group();
        let s = stream();
        let owner_a = OwnerId::new("worker-a").unwrap();
        let lease_dur = std::time::Duration::from_secs(60);

        let lease = coord
            .claim(&g, &s, 0, &owner_a, lease_dur)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lease.generation, 1);

        coord
            .release(&g, &s, 0, &owner_a, lease.generation)
            .await
            .unwrap();

        let err = coord
            .checkpoint(&g, &s, 0, &owner_a, lease.generation, 100)
            .await
            .unwrap_err();
        assert!(matches!(err, eventuary_core::Error::OwnershipLost(_)));
    }
}
