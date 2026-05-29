use std::sync::Arc;

use chrono::{Duration, Utc};

use eventuary_core::io::OwnerId;
use eventuary_core::io::reader::{
    CheckpointScope, Generation, PartitionCoordinator, PartitionLease,
};
use eventuary_core::{Error, Partition, Result};

use crate::database::SqliteConn;
use crate::reader::SqliteCursor;
use crate::relation::SqliteRelationName;
use crate::schema::{Migration, RelationReplacement};

const PARTITION_COORDINATOR_0001_INIT_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS {consumers} (
    consumer_group_id TEXT NOT NULL,
    stream_id         TEXT NOT NULL,
    owner_id          TEXT NOT NULL,
    lease_until       TEXT NOT NULL,
    PRIMARY KEY (consumer_group_id, stream_id, owner_id)
);

CREATE INDEX IF NOT EXISTS idx_event_stream_consumers_group_stream_lease
ON {consumers} (consumer_group_id, stream_id, lease_until);

CREATE TABLE IF NOT EXISTS {partitions} (
    consumer_group_id   TEXT    NOT NULL,
    stream_id           TEXT    NOT NULL,
    partition_id        INTEGER NOT NULL,
    partition_count     INTEGER NULL,
    owner_id            TEXT    NULL,
    lease_until         TEXT    NULL,
    checkpoint_sequence INTEGER NOT NULL DEFAULT 0,
    generation          INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (consumer_group_id, stream_id, partition_id)
);

CREATE INDEX IF NOT EXISTS idx_event_stream_partitions_group_stream_owner
ON {partitions} (consumer_group_id, stream_id, owner_id);

CREATE INDEX IF NOT EXISTS idx_event_stream_partitions_group_stream_count
ON {partitions} (consumer_group_id, stream_id, partition_count, partition_id);
"#;

const PARTITION_COORDINATOR_MIGRATIONS: &[Migration] = &[Migration {
    name: "0001_init",
    sql: PARTITION_COORDINATOR_0001_INIT_SQL,
}];

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

    pub fn connect(conn: SqliteConn, config: SqlitePartitionCoordinatorConfig) -> Result<Self> {
        Self::prepare_schema(&conn, &config)?;
        Ok(Self::new(conn, config))
    }

    pub fn prepare_schema(
        conn: &SqliteConn,
        config: &SqlitePartitionCoordinatorConfig,
    ) -> Result<()> {
        let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
        crate::schema::apply_schema(
            &guard,
            PARTITION_COORDINATOR_MIGRATIONS,
            &[
                RelationReplacement {
                    token: "{consumers}",
                    relation: &config.consumers_relation,
                },
                RelationReplacement {
                    token: "{partitions}",
                    relation: &config.partitions_relation,
                },
            ],
        )
    }

    pub fn schema_sql(config: &SqlitePartitionCoordinatorConfig) -> String {
        crate::schema::render_schema_sql(
            PARTITION_COORDINATOR_MIGRATIONS,
            &[
                RelationReplacement {
                    token: "{consumers}",
                    relation: &config.consumers_relation,
                },
                RelationReplacement {
                    token: "{partitions}",
                    relation: &config.partitions_relation,
                },
            ],
        )
    }
}

fn compute_lease_until(lease_duration: std::time::Duration) -> Result<String> {
    let dt = Utc::now()
        + Duration::from_std(lease_duration)
            .map_err(|_| Error::Config("lease duration out of range".to_owned()))?;
    Ok(dt.to_rfc3339())
}

impl PartitionCoordinator<SqliteCursor> for SqlitePartitionCoordinator {
    async fn heartbeat<'a>(
        &'a self,
        scope: &'a CheckpointScope,
        owner_id: &'a OwnerId,
        lease_duration: std::time::Duration,
    ) -> Result<()> {
        let lease_until = compute_lease_until(lease_duration)?;
        let conn = Arc::clone(&self.conn);
        let consumers = Arc::clone(&self.consumers_relation);
        let group = scope.consumer_group_id.as_str().to_owned();
        let stream = scope.stream_id.as_str().to_owned();
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

    async fn live_consumers<'a>(&'a self, scope: &'a CheckpointScope) -> Result<usize> {
        let conn = Arc::clone(&self.conn);
        let consumers = Arc::clone(&self.consumers_relation);
        let group = scope.consumer_group_id.as_str().to_owned();
        let stream = scope.stream_id.as_str().to_owned();
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

    async fn release_consumer<'a>(
        &'a self,
        scope: &'a CheckpointScope,
        owner_id: &'a OwnerId,
    ) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        let consumers = Arc::clone(&self.consumers_relation);
        let group = scope.consumer_group_id.as_str().to_owned();
        let stream = scope.stream_id.as_str().to_owned();
        let owner = owner_id.as_str().to_owned();
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            let sql = format!(
                "DELETE FROM {consumers} \
                 WHERE consumer_group_id = ?1 AND stream_id = ?2 AND owner_id = ?3"
            );
            guard
                .execute(&sql, rusqlite::params![group, stream, owner])
                .map_err(|e| Error::Store(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
    }

    async fn claim<'a>(
        &'a self,
        scope: &'a CheckpointScope,
        owner_id: &'a OwnerId,
        partition: Partition,
        lease_duration: std::time::Duration,
    ) -> Result<Option<PartitionLease<SqliteCursor>>> {
        let lease_until = compute_lease_until(lease_duration)?;
        let conn = Arc::clone(&self.conn);
        let partitions = Arc::clone(&self.partitions_relation);
        let group = scope.consumer_group_id.as_str().to_owned();
        let stream = scope.stream_id.as_str().to_owned();
        let owner = owner_id.as_str().to_owned();
        let partition_id_i64 = partition.id() as i64;
        let partition_count_i64 = partition.count() as i64;
        let now = Utc::now().to_rfc3339();
        let scope = scope.clone();
        let owner_id = owner_id.clone();
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            let sql = format!(
                "INSERT INTO {partitions} \
                   (consumer_group_id, stream_id, partition_id, partition_count, owner_id, lease_until, generation, checkpoint_sequence) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, 0) \
                 ON CONFLICT (consumer_group_id, stream_id, partition_id) DO UPDATE \
                 SET owner_id = excluded.owner_id, \
                     lease_until = excluded.lease_until, \
                     partition_count = COALESCE({partitions}.partition_count, excluded.partition_count), \
                     generation = {partitions}.generation + 1 \
                 WHERE ({partitions}.partition_count IS NULL OR {partitions}.partition_count = excluded.partition_count) \
                   AND ({partitions}.owner_id IS NULL \
                        OR {partitions}.lease_until IS NULL \
                        OR {partitions}.lease_until < ?7 \
                        OR {partitions}.owner_id = excluded.owner_id) \
                 RETURNING owner_id, lease_until, generation, checkpoint_sequence"
            );
            let row = guard
                .query_row(
                    &sql,
                    rusqlite::params![
                        group,
                        stream,
                        partition_id_i64,
                        partition_count_i64,
                        owner,
                        lease_until,
                        now
                    ],
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
                None => {
                    let check_sql = format!(
                        "SELECT partition_count FROM {partitions} \
                         WHERE consumer_group_id = ?1 AND stream_id = ?2 AND partition_id = ?3"
                    );
                    let stored: Option<i64> = guard
                        .query_row(
                            &check_sql,
                            rusqlite::params![group, stream, partition_id_i64],
                            |r| r.get::<_, Option<i64>>(0),
                        )
                        .map(Some)
                        .or_else(|e| match e {
                            rusqlite::Error::QueryReturnedNoRows => Ok(None),
                            other => Err(other),
                        })
                        .map_err(|e| Error::Store(e.to_string()))?
                        .flatten();
                    if let Some(stored) = stored
                        && stored != partition_count_i64
                    {
                        return Err(Error::Config(format!(
                            "partition count mismatch for scope {} stream {} partition {}: stored {}, requested {}",
                            group, stream, partition_id_i64, stored, partition_count_i64,
                        )));
                    }
                    Ok(None)
                }
                Some((_returned_owner, lease_until_text, generation, checkpoint_sequence)) => {
                    let lease_until = chrono::DateTime::parse_from_rfc3339(&lease_until_text)
                        .map(|dt| dt.with_timezone(&Utc))
                        .map_err(|e| {
                            Error::Serialization(format!("lease_until decode: {e}"))
                        })?;
                    Ok(Some(PartitionLease {
                        scope,
                        owner_id,
                        partition,
                        generation: Generation::from_i64(generation),
                        checkpoint_cursor: (checkpoint_sequence > 0)
                            .then_some(SqliteCursor::new(checkpoint_sequence, partition)),
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
        lease: &'a PartitionLease<SqliteCursor>,
        lease_duration: std::time::Duration,
    ) -> Result<()> {
        let lease_until = compute_lease_until(lease_duration)?;
        let conn = Arc::clone(&self.conn);
        let partitions = Arc::clone(&self.partitions_relation);
        let group = lease.scope.consumer_group_id.as_str().to_owned();
        let stream = lease.scope.stream_id.as_str().to_owned();
        let owner = lease.owner_id.as_str().to_owned();
        let partition_id_i64 = lease.partition.id() as i64;
        let partition_count_i64 = lease.partition.count() as i64;
        let generation = lease.generation.get();
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            let sql = format!(
                "UPDATE {partitions} \
                 SET lease_until = ?5 \
                 WHERE consumer_group_id = ?1 \
                   AND stream_id = ?2 \
                   AND partition_id = ?3 \
                   AND owner_id = ?4 \
                   AND generation = ?6 \
                   AND partition_count = ?7"
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
                        generation,
                        partition_count_i64
                    ],
                )
                .map_err(|e| Error::Store(e.to_string()))?;
            if affected == 0 {
                check_partition_count_mismatch(
                    &guard,
                    &partitions,
                    &group,
                    &stream,
                    partition_id_i64,
                    partition_count_i64,
                )?;
                return Err(Error::OwnershipLost(format!(
                    "partition {partition_id_i64} generation {generation}"
                )));
            }
            Ok(())
        })
        .await
        .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
    }

    async fn release<'a>(&'a self, lease: &'a PartitionLease<SqliteCursor>) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        let partitions = Arc::clone(&self.partitions_relation);
        let group = lease.scope.consumer_group_id.as_str().to_owned();
        let stream = lease.scope.stream_id.as_str().to_owned();
        let owner = lease.owner_id.as_str().to_owned();
        let partition_id_i64 = lease.partition.id() as i64;
        let partition_count_i64 = lease.partition.count() as i64;
        let generation = lease.generation.get();
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
                   AND generation = ?5 \
                   AND partition_count = ?6"
            );
            let affected = guard
                .execute(
                    &sql,
                    rusqlite::params![
                        group,
                        stream,
                        partition_id_i64,
                        owner,
                        generation,
                        partition_count_i64
                    ],
                )
                .map_err(|e| Error::Store(e.to_string()))?;
            if affected == 0 {
                check_partition_count_mismatch(
                    &guard,
                    &partitions,
                    &group,
                    &stream,
                    partition_id_i64,
                    partition_count_i64,
                )?;
                return Err(Error::OwnershipLost(format!(
                    "partition {partition_id_i64} generation {generation}"
                )));
            }
            Ok(())
        })
        .await
        .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
    }

    async fn checkpoint<'a>(
        &'a self,
        lease: &'a PartitionLease<SqliteCursor>,
        cursor: SqliteCursor,
    ) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        let partitions = Arc::clone(&self.partitions_relation);
        let group = lease.scope.consumer_group_id.as_str().to_owned();
        let stream = lease.scope.stream_id.as_str().to_owned();
        let owner = lease.owner_id.as_str().to_owned();
        let partition_id_i64 = lease.partition.id() as i64;
        let partition_count_i64 = lease.partition.count() as i64;
        let generation = lease.generation.get();
        let sequence = cursor.sequence;
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
                   AND partition_count = ?7 \
                   AND ?6 > {partitions}.checkpoint_sequence"
            );
            let affected = guard
                .execute(
                    &sql,
                    rusqlite::params![
                        group,
                        stream,
                        partition_id_i64,
                        owner,
                        generation,
                        sequence,
                        partition_count_i64
                    ],
                )
                .map_err(|e| Error::Store(e.to_string()))?;
            if affected == 0 {
                let check_sql = format!(
                    "SELECT owner_id, generation, partition_count FROM {partitions} \
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
                                r.get::<_, Option<i64>>(2)?,
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
                    Some((current_owner, current_generation, current_count)) => {
                        if let Some(stored) = current_count
                            && stored != partition_count_i64
                        {
                            return Err(Error::Config(format!(
                                "partition count mismatch for scope {group} stream {stream} partition {partition_id_i64}: stored {stored}, requested {partition_count_i64}"
                            )));
                        }
                        if current_generation == generation
                            && current_owner.as_deref() == Some(owner.as_str())
                        {
                            return Ok(());
                        }
                        Err(Error::OwnershipLost(format!(
                            "checkpoint rejected for partition {partition_id_i64}: stale owner/generation"
                        )))
                    }
                    None => Err(Error::OwnershipLost(format!(
                        "partition {partition_id_i64} generation {generation}"
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

fn check_partition_count_mismatch(
    guard: &rusqlite::Connection,
    partitions: &str,
    group: &str,
    stream: &str,
    partition_id: i64,
    requested_count: i64,
) -> Result<()> {
    let sql = format!(
        "SELECT partition_count FROM {partitions} \
         WHERE consumer_group_id = ?1 AND stream_id = ?2 AND partition_id = ?3"
    );
    let stored: Option<i64> = guard
        .query_row(&sql, rusqlite::params![group, stream, partition_id], |r| {
            r.get::<_, Option<i64>>(0)
        })
        .map(Some)
        .or_else(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => Ok(None),
            other => Err(other),
        })
        .map_err(|e| Error::Store(e.to_string()))?
        .flatten();
    if let Some(stored) = stored
        && stored != requested_count
    {
        return Err(Error::Config(format!(
            "partition count mismatch for scope {group} stream {stream} partition {partition_id}: stored {stored}, requested {requested_count}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::*;
    use eventuary_core::io::reader::PartitionCoordinator;
    use eventuary_core::io::{ConsumerGroupId, OwnerId, StreamId};

    use crate::database::SqliteDatabase;

    #[test]
    fn schema_sql_contains_expected_tables() {
        let sql =
            SqlitePartitionCoordinator::schema_sql(&SqlitePartitionCoordinatorConfig::default());
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"event_stream_consumers\""));
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"event_stream_partitions\""));
    }

    fn make_coordinator() -> SqlitePartitionCoordinator {
        let db = SqliteDatabase::open_in_memory().unwrap();
        let conn = db.conn();
        SqlitePartitionCoordinator::prepare_schema(
            &conn,
            &SqlitePartitionCoordinatorConfig::default(),
        )
        .unwrap();
        SqlitePartitionCoordinator::new(conn, SqlitePartitionCoordinatorConfig::default())
    }

    fn scope() -> CheckpointScope {
        CheckpointScope::new(
            ConsumerGroupId::new("group-1").unwrap(),
            StreamId::new("orders").unwrap(),
        )
    }

    fn partition(id: u32) -> Partition {
        Partition::new(id, NonZeroU32::new(64).unwrap()).unwrap()
    }

    #[tokio::test]
    async fn heartbeat_and_live_count() {
        let coord = make_coordinator();
        let s = scope();
        let owner_a = OwnerId::new("worker-a").unwrap();
        let owner_b = OwnerId::new("worker-b").unwrap();
        let long_lease = std::time::Duration::from_secs(60);

        coord.heartbeat(&s, &owner_a, long_lease).await.unwrap();
        coord.heartbeat(&s, &owner_b, long_lease).await.unwrap();

        let live = coord.live_consumers(&s).await.unwrap();
        assert_eq!(live, 2);
    }

    #[tokio::test]
    async fn live_consumers_excludes_expired() {
        let coord = make_coordinator();
        let s = scope();
        let owner_a = OwnerId::new("worker-a").unwrap();
        let owner_b = OwnerId::new("worker-b").unwrap();
        let long_lease = std::time::Duration::from_secs(60);
        let short_lease = std::time::Duration::from_millis(50);

        coord.heartbeat(&s, &owner_a, long_lease).await.unwrap();
        coord.heartbeat(&s, &owner_b, short_lease).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let live = coord.live_consumers(&s).await.unwrap();
        assert_eq!(live, 1);
    }

    #[tokio::test]
    async fn claim_free_partition_succeeds() {
        let coord = make_coordinator();
        let s = scope();
        let owner = OwnerId::new("worker-a").unwrap();
        let lease_dur = std::time::Duration::from_secs(60);

        let lease = coord
            .claim(&s, &owner, partition(0), lease_dur)
            .await
            .unwrap()
            .expect("should get lease");

        assert_eq!(lease.generation.get(), 1);
        assert!(lease.checkpoint_cursor.is_none());
    }

    #[tokio::test]
    async fn claim_contested_returns_none() {
        let coord = make_coordinator();
        let s = scope();
        let owner_a = OwnerId::new("worker-a").unwrap();
        let owner_b = OwnerId::new("worker-b").unwrap();
        let lease_dur = std::time::Duration::from_secs(60);

        let first = coord
            .claim(&s, &owner_a, partition(0), lease_dur)
            .await
            .unwrap();
        assert!(first.is_some());

        let second = coord
            .claim(&s, &owner_b, partition(0), lease_dur)
            .await
            .unwrap();
        assert!(second.is_none());
    }

    #[tokio::test]
    async fn claim_after_expiry_succeeds() {
        let coord = make_coordinator();
        let s = scope();
        let owner_a = OwnerId::new("worker-a").unwrap();
        let owner_b = OwnerId::new("worker-b").unwrap();
        let short_lease = std::time::Duration::from_millis(50);
        let long_lease = std::time::Duration::from_secs(60);

        let first = coord
            .claim(&s, &owner_a, partition(0), short_lease)
            .await
            .unwrap();
        assert_eq!(first.unwrap().generation.get(), 1);

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let second = coord
            .claim(&s, &owner_b, partition(0), long_lease)
            .await
            .unwrap()
            .expect("should succeed after expiry");
        assert_eq!(second.generation.get(), 2);
    }

    #[tokio::test]
    async fn renew_with_matching_generation() {
        let coord = make_coordinator();
        let s = scope();
        let owner = OwnerId::new("worker-a").unwrap();
        let lease_dur = std::time::Duration::from_secs(60);

        let lease = coord
            .claim(&s, &owner, partition(0), lease_dur)
            .await
            .unwrap()
            .unwrap();

        coord.renew(&lease, lease_dur).await.unwrap();
    }

    #[tokio::test]
    async fn renew_with_stale_generation_returns_ownership_lost() {
        let coord = make_coordinator();
        let s = scope();
        let owner_a = OwnerId::new("worker-a").unwrap();
        let owner_b = OwnerId::new("worker-b").unwrap();
        let short_lease = std::time::Duration::from_millis(50);
        let long_lease = std::time::Duration::from_secs(60);

        let lease_a = coord
            .claim(&s, &owner_a, partition(0), short_lease)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lease_a.generation.get(), 1);

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        coord
            .claim(&s, &owner_b, partition(0), long_lease)
            .await
            .unwrap()
            .unwrap();

        let err = coord.renew(&lease_a, long_lease).await.unwrap_err();
        assert!(matches!(err, eventuary_core::Error::OwnershipLost(_)));
    }

    #[tokio::test]
    async fn release_with_matching_generation() {
        let coord = make_coordinator();
        let s = scope();
        let owner_a = OwnerId::new("worker-a").unwrap();
        let owner_b = OwnerId::new("worker-b").unwrap();
        let lease_dur = std::time::Duration::from_secs(60);

        let lease = coord
            .claim(&s, &owner_a, partition(0), lease_dur)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lease.generation.get(), 1);

        coord.release(&lease).await.unwrap();

        let new_lease = coord
            .claim(&s, &owner_b, partition(0), lease_dur)
            .await
            .unwrap()
            .expect("should claim after release");
        assert_eq!(new_lease.generation.get(), 3);
    }

    #[tokio::test]
    async fn release_with_stale_generation_returns_ownership_lost() {
        let coord = make_coordinator();
        let s = scope();
        let owner_a = OwnerId::new("worker-a").unwrap();
        let owner_b = OwnerId::new("worker-b").unwrap();
        let short_lease = std::time::Duration::from_millis(50);
        let long_lease = std::time::Duration::from_secs(60);

        let lease_a = coord
            .claim(&s, &owner_a, partition(0), short_lease)
            .await
            .unwrap()
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        coord
            .claim(&s, &owner_b, partition(0), long_lease)
            .await
            .unwrap()
            .unwrap();

        let err = coord.release(&lease_a).await.unwrap_err();
        assert!(matches!(err, eventuary_core::Error::OwnershipLost(_)));
    }

    #[tokio::test]
    async fn checkpoint_with_matching_generation_advances() {
        let coord = make_coordinator();
        let s = scope();
        let owner_a = OwnerId::new("worker-a").unwrap();
        let owner_b = OwnerId::new("worker-b").unwrap();
        let long_lease = std::time::Duration::from_secs(60);

        let lease = coord
            .claim(&s, &owner_a, partition(0), long_lease)
            .await
            .unwrap()
            .unwrap();

        coord
            .checkpoint(&lease, SqliteCursor::new(100, partition(0)))
            .await
            .unwrap();

        coord.release(&lease).await.unwrap();

        let new_lease = coord
            .claim(&s, &owner_b, partition(0), long_lease)
            .await
            .unwrap()
            .expect("should claim after release");
        assert_eq!(new_lease.checkpoint_cursor.unwrap().sequence, 100);
    }

    #[tokio::test]
    async fn checkpoint_is_monotonic() {
        let coord = make_coordinator();
        let s = scope();
        let owner_a = OwnerId::new("worker-a").unwrap();
        let owner_b = OwnerId::new("worker-b").unwrap();
        let long_lease = std::time::Duration::from_secs(60);

        let lease = coord
            .claim(&s, &owner_a, partition(0), long_lease)
            .await
            .unwrap()
            .unwrap();

        coord
            .checkpoint(&lease, SqliteCursor::new(100, partition(0)))
            .await
            .unwrap();
        coord
            .checkpoint(&lease, SqliteCursor::new(50, partition(0)))
            .await
            .unwrap();

        coord.release(&lease).await.unwrap();

        let new_lease = coord
            .claim(&s, &owner_b, partition(0), long_lease)
            .await
            .unwrap()
            .expect("should claim after release");
        assert_eq!(new_lease.checkpoint_cursor.unwrap().sequence, 100);
    }

    #[tokio::test]
    async fn checkpoint_with_stale_generation_returns_ownership_lost() {
        let coord = make_coordinator();
        let s = scope();
        let owner_a = OwnerId::new("worker-a").unwrap();
        let owner_b = OwnerId::new("worker-b").unwrap();
        let short_lease = std::time::Duration::from_millis(50);
        let long_lease = std::time::Duration::from_secs(60);

        let lease_a = coord
            .claim(&s, &owner_a, partition(0), short_lease)
            .await
            .unwrap()
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        coord
            .claim(&s, &owner_b, partition(0), long_lease)
            .await
            .unwrap()
            .unwrap();

        let err = coord
            .checkpoint(&lease_a, SqliteCursor::new(100, partition(0)))
            .await
            .unwrap_err();
        assert!(matches!(err, eventuary_core::Error::OwnershipLost(_)));
    }

    #[tokio::test]
    async fn claim_rejects_partition_count_mismatch() {
        let coord = make_coordinator();
        let s = scope();
        let owner_a = OwnerId::new("worker-a").unwrap();
        let owner_b = OwnerId::new("worker-b").unwrap();
        let p_four = Partition::new(0, NonZeroU32::new(4).unwrap()).unwrap();
        let p_eight = Partition::new(0, NonZeroU32::new(8).unwrap()).unwrap();

        coord
            .claim(&s, &owner_a, p_four, std::time::Duration::from_millis(1))
            .await
            .unwrap()
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        let err = coord
            .claim(&s, &owner_b, p_eight, std::time::Duration::from_secs(10))
            .await
            .unwrap_err();
        assert!(
            matches!(err, eventuary_core::Error::Config(ref message) if message.contains("partition count mismatch")),
            "expected partition count mismatch error, got {err:?}"
        );
    }

    #[tokio::test]
    async fn checkpoint_after_release_returns_ownership_lost() {
        let coord = make_coordinator();
        let s = scope();
        let owner_a = OwnerId::new("worker-a").unwrap();
        let lease_dur = std::time::Duration::from_secs(60);

        let lease = coord
            .claim(&s, &owner_a, partition(0), lease_dur)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lease.generation.get(), 1);

        coord.release(&lease).await.unwrap();

        let err = coord
            .checkpoint(&lease, SqliteCursor::new(100, partition(0)))
            .await
            .unwrap_err();
        assert!(matches!(err, eventuary_core::Error::OwnershipLost(_)));
    }
}
