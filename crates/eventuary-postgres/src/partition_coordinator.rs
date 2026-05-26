use std::sync::Arc;

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};

use eventuary_core::io::OwnerId;
use eventuary_core::io::reader::{
    CheckpointScope, Generation, PartitionCoordinator, PartitionLease,
};
use eventuary_core::{Error, Partition, Result};

use crate::reader::PgCursor;
use crate::relation::PgRelationName;
use crate::schema::{Migration, RelationReplacement};

const PARTITION_COORDINATOR_0001_INIT_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS {consumers} (
    consumer_group_id TEXT NOT NULL,
    stream_id         TEXT NOT NULL,
    owner_id          TEXT NOT NULL,
    lease_until       TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (consumer_group_id, stream_id, owner_id)
);

CREATE INDEX IF NOT EXISTS idx_event_stream_consumers_group_stream_lease
ON {consumers} (consumer_group_id, stream_id, lease_until);

CREATE TABLE IF NOT EXISTS {partitions} (
    consumer_group_id   TEXT        NOT NULL,
    stream_id           TEXT        NOT NULL,
    partition_id        BIGINT      NOT NULL,
    partition_count     BIGINT      NULL,
    owner_id            TEXT        NULL,
    lease_until         TIMESTAMPTZ NULL,
    checkpoint_sequence BIGINT      NOT NULL DEFAULT 0,
    generation          BIGINT      NOT NULL DEFAULT 0,
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
pub struct PgPartitionCoordinatorConfig {
    pub consumers_relation: PgRelationName,
    pub partitions_relation: PgRelationName,
}

impl Default for PgPartitionCoordinatorConfig {
    fn default() -> Self {
        Self {
            consumers_relation: PgRelationName::new("event_stream_consumers")
                .expect("default consumers relation"),
            partitions_relation: PgRelationName::new("event_stream_partitions")
                .expect("default partitions relation"),
        }
    }
}

pub struct PgPartitionCoordinator {
    pool: PgPool,
    consumers_relation: Arc<String>,
    partitions_relation: Arc<String>,
}

impl Clone for PgPartitionCoordinator {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            consumers_relation: Arc::clone(&self.consumers_relation),
            partitions_relation: Arc::clone(&self.partitions_relation),
        }
    }
}

impl PgPartitionCoordinator {
    pub fn new(pool: PgPool, config: PgPartitionCoordinatorConfig) -> Self {
        Self {
            pool,
            consumers_relation: Arc::new(config.consumers_relation.render()),
            partitions_relation: Arc::new(config.partitions_relation.render()),
        }
    }

    pub async fn connect(pool: PgPool, config: PgPartitionCoordinatorConfig) -> Result<Self> {
        Self::prepare_schema(&pool, &config).await?;
        Ok(Self::new(pool, config))
    }

    pub async fn prepare_schema(
        pool: &PgPool,
        config: &PgPartitionCoordinatorConfig,
    ) -> Result<()> {
        crate::schema::apply_schema(
            pool,
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
        .await
    }

    pub fn schema_sql(config: &PgPartitionCoordinatorConfig) -> String {
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

fn compute_lease_until(lease_duration: std::time::Duration) -> Result<DateTime<Utc>> {
    Ok(Utc::now()
        + chrono::Duration::from_std(lease_duration)
            .map_err(|_| Error::Config("lease duration out of range".to_owned()))?)
}

fn lease_until_to_sql(dt: DateTime<Utc>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
}

fn parse_lease_until(s: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|_| {
            DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%#z").map(|dt| dt.with_timezone(&Utc))
        })
        .map_err(|e| Error::Serialization(format!("lease_until decode: {e}")))
}

impl PartitionCoordinator<PgCursor> for PgPartitionCoordinator {
    async fn heartbeat<'a>(
        &'a self,
        scope: &'a CheckpointScope,
        owner_id: &'a OwnerId,
        lease_duration: std::time::Duration,
    ) -> Result<()> {
        let lease_until = lease_until_to_sql(compute_lease_until(lease_duration)?);
        let sql = format!(
            "INSERT INTO {consumers} (consumer_group_id, stream_id, owner_id, lease_until) \
             VALUES ($1, $2, $3, $4::timestamptz) \
             ON CONFLICT (consumer_group_id, stream_id, owner_id) DO UPDATE \
             SET lease_until = EXCLUDED.lease_until",
            consumers = self.consumers_relation
        );
        sqlx::query(&sql)
            .bind(scope.consumer_group_id.as_str())
            .bind(scope.stream_id.as_str())
            .bind(owner_id.as_str())
            .bind(lease_until)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        Ok(())
    }

    async fn live_consumers<'a>(&'a self, scope: &'a CheckpointScope) -> Result<usize> {
        let sql = format!(
            "SELECT COUNT(*) FROM {consumers} \
             WHERE consumer_group_id = $1 \
               AND stream_id = $2 \
               AND lease_until > NOW()",
            consumers = self.consumers_relation
        );
        let row = sqlx::query(&sql)
            .bind(scope.consumer_group_id.as_str())
            .bind(scope.stream_id.as_str())
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        let count: i64 = row.get(0);
        Ok(count as usize)
    }

    async fn release_consumer<'a>(
        &'a self,
        scope: &'a CheckpointScope,
        owner_id: &'a OwnerId,
    ) -> Result<()> {
        let sql = format!(
            "DELETE FROM {consumers} \
             WHERE consumer_group_id = $1 AND stream_id = $2 AND owner_id = $3",
            consumers = self.consumers_relation
        );
        sqlx::query(&sql)
            .bind(scope.consumer_group_id.as_str())
            .bind(scope.stream_id.as_str())
            .bind(owner_id.as_str())
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        Ok(())
    }

    async fn claim<'a>(
        &'a self,
        scope: &'a CheckpointScope,
        owner_id: &'a OwnerId,
        partition: Partition,
        lease_duration: std::time::Duration,
    ) -> Result<Option<PartitionLease<PgCursor>>> {
        let lease_until = lease_until_to_sql(compute_lease_until(lease_duration)?);
        let partition_id_i64 = partition.id() as i64;
        let partition_count_i64 = partition.count() as i64;
        let sql = format!(
            "INSERT INTO {partitions} \
                (consumer_group_id, stream_id, partition_id, partition_count, owner_id, lease_until, generation, checkpoint_sequence) \
             VALUES ($1, $2, $3, $4, $5, $6::timestamptz, 1, 0) \
             ON CONFLICT (consumer_group_id, stream_id, partition_id) DO UPDATE \
             SET owner_id = EXCLUDED.owner_id, \
                 lease_until = EXCLUDED.lease_until, \
                 partition_count = COALESCE({partitions}.partition_count, EXCLUDED.partition_count), \
                 generation = {partitions}.generation + 1 \
             WHERE ({partitions}.partition_count IS NULL OR {partitions}.partition_count = EXCLUDED.partition_count) \
               AND ({partitions}.owner_id IS NULL \
                    OR {partitions}.lease_until IS NULL \
                    OR {partitions}.lease_until < NOW() \
                    OR {partitions}.owner_id = EXCLUDED.owner_id) \
             RETURNING owner_id, \
                       to_char(lease_until AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') AS lease_until_text, \
                       generation, \
                       checkpoint_sequence",
            partitions = self.partitions_relation
        );
        let row = sqlx::query(&sql)
            .bind(scope.consumer_group_id.as_str())
            .bind(scope.stream_id.as_str())
            .bind(partition_id_i64)
            .bind(partition_count_i64)
            .bind(owner_id.as_str())
            .bind(lease_until)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        match row {
            None => {
                let check_sql = format!(
                    "SELECT partition_count FROM {partitions} \
                     WHERE consumer_group_id = $1 AND stream_id = $2 AND partition_id = $3",
                    partitions = self.partitions_relation
                );
                let check_row = sqlx::query(&check_sql)
                    .bind(scope.consumer_group_id.as_str())
                    .bind(scope.stream_id.as_str())
                    .bind(partition_id_i64)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(|e| Error::Store(e.to_string()))?;
                if let Some(r) = check_row {
                    let stored: Option<i64> = r.get("partition_count");
                    if let Some(stored) = stored
                        && stored != partition_count_i64
                    {
                        return Err(Error::Config(format!(
                            "partition count mismatch for scope {} stream {} partition {}: stored {}, requested {}",
                            scope.consumer_group_id.as_str(),
                            scope.stream_id.as_str(),
                            partition.id(),
                            stored,
                            partition_count_i64,
                        )));
                    }
                }
                Ok(None)
            }
            Some(r) => {
                let lease_until_text: String = r.get("lease_until_text");
                let returned_lease_until = parse_lease_until(&lease_until_text)?;
                let generation: i64 = r.get("generation");
                let checkpoint_sequence: i64 = r.get("checkpoint_sequence");
                Ok(Some(PartitionLease {
                    scope: scope.clone(),
                    owner_id: owner_id.clone(),
                    partition,
                    generation: Generation::from_i64(generation),
                    checkpoint_cursor: (checkpoint_sequence > 0)
                        .then_some(PgCursor::new(checkpoint_sequence)),
                    lease_until: returned_lease_until,
                }))
            }
        }
    }

    async fn renew<'a>(
        &'a self,
        lease: &'a PartitionLease<PgCursor>,
        lease_duration: std::time::Duration,
    ) -> Result<()> {
        let lease_until = lease_until_to_sql(compute_lease_until(lease_duration)?);
        let partition_id_i64 = lease.partition.id() as i64;
        let partition_count_i64 = lease.partition.count() as i64;
        let generation = lease.generation.get();
        let sql = format!(
            "UPDATE {partitions} \
             SET lease_until = $5::timestamptz \
             WHERE consumer_group_id = $1 \
               AND stream_id = $2 \
               AND partition_id = $3 \
               AND owner_id = $4 \
               AND generation = $6 \
               AND partition_count = $7",
            partitions = self.partitions_relation
        );
        let result = sqlx::query(&sql)
            .bind(lease.scope.consumer_group_id.as_str())
            .bind(lease.scope.stream_id.as_str())
            .bind(partition_id_i64)
            .bind(lease.owner_id.as_str())
            .bind(lease_until)
            .bind(generation)
            .bind(partition_count_i64)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        if result.rows_affected() == 0 {
            self.check_partition_count_mismatch(lease).await?;
            return Err(Error::OwnershipLost(format!(
                "partition {} generation {}",
                lease.partition.id(),
                lease.generation,
            )));
        }
        Ok(())
    }

    async fn release<'a>(&'a self, lease: &'a PartitionLease<PgCursor>) -> Result<()> {
        let partition_id_i64 = lease.partition.id() as i64;
        let partition_count_i64 = lease.partition.count() as i64;
        let generation = lease.generation.get();
        let sql = format!(
            "UPDATE {partitions} \
             SET owner_id = NULL, \
                 lease_until = NULL, \
                 generation = generation + 1 \
             WHERE consumer_group_id = $1 \
               AND stream_id = $2 \
               AND partition_id = $3 \
               AND owner_id = $4 \
               AND generation = $5 \
               AND partition_count = $6",
            partitions = self.partitions_relation
        );
        let result = sqlx::query(&sql)
            .bind(lease.scope.consumer_group_id.as_str())
            .bind(lease.scope.stream_id.as_str())
            .bind(partition_id_i64)
            .bind(lease.owner_id.as_str())
            .bind(generation)
            .bind(partition_count_i64)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        if result.rows_affected() == 0 {
            self.check_partition_count_mismatch(lease).await?;
            return Err(Error::OwnershipLost(format!(
                "partition {} generation {}",
                lease.partition.id(),
                lease.generation,
            )));
        }
        Ok(())
    }

    async fn checkpoint<'a>(
        &'a self,
        lease: &'a PartitionLease<PgCursor>,
        cursor: PgCursor,
    ) -> Result<()> {
        let partition_id_i64 = lease.partition.id() as i64;
        let partition_count_i64 = lease.partition.count() as i64;
        let generation = lease.generation.get();
        let sequence = cursor.sequence;
        let sql = format!(
            "UPDATE {partitions} \
             SET checkpoint_sequence = $6 \
             WHERE consumer_group_id = $1 \
               AND stream_id = $2 \
               AND partition_id = $3 \
               AND owner_id = $4 \
               AND generation = $5 \
               AND partition_count = $7 \
               AND $6 > {partitions}.checkpoint_sequence",
            partitions = self.partitions_relation
        );
        let result = sqlx::query(&sql)
            .bind(lease.scope.consumer_group_id.as_str())
            .bind(lease.scope.stream_id.as_str())
            .bind(partition_id_i64)
            .bind(lease.owner_id.as_str())
            .bind(generation)
            .bind(sequence)
            .bind(partition_count_i64)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        if result.rows_affected() == 0 {
            let check_sql = format!(
                "SELECT generation, owner_id, partition_count FROM {partitions} \
                 WHERE consumer_group_id = $1 AND stream_id = $2 AND partition_id = $3",
                partitions = self.partitions_relation
            );
            let check_row = sqlx::query(&check_sql)
                .bind(lease.scope.consumer_group_id.as_str())
                .bind(lease.scope.stream_id.as_str())
                .bind(partition_id_i64)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| Error::Store(e.to_string()))?;
            match check_row {
                Some(r) => {
                    let current_generation: i64 = r.get("generation");
                    let current_owner: Option<String> = r.get("owner_id");
                    let current_count: Option<i64> = r.get("partition_count");
                    if let Some(stored) = current_count
                        && stored != partition_count_i64
                    {
                        return Err(Error::Config(format!(
                            "partition count mismatch for scope {} stream {} partition {}: stored {}, requested {}",
                            lease.scope.consumer_group_id.as_str(),
                            lease.scope.stream_id.as_str(),
                            lease.partition.id(),
                            stored,
                            partition_count_i64,
                        )));
                    }
                    if current_generation == generation
                        && current_owner.as_deref() == Some(lease.owner_id.as_str())
                    {
                        return Ok(());
                    }
                    Err(Error::OwnershipLost(format!(
                        "checkpoint rejected for partition {}: stale owner/generation",
                        lease.partition.id(),
                    )))
                }
                None => Err(Error::OwnershipLost(format!(
                    "partition {} generation {}",
                    lease.partition.id(),
                    lease.generation,
                ))),
            }
        } else {
            Ok(())
        }
    }
}

impl PgPartitionCoordinator {
    async fn check_partition_count_mismatch(&self, lease: &PartitionLease<PgCursor>) -> Result<()> {
        let partition_id_i64 = lease.partition.id() as i64;
        let partition_count_i64 = lease.partition.count() as i64;
        let sql = format!(
            "SELECT partition_count FROM {partitions} \
             WHERE consumer_group_id = $1 AND stream_id = $2 AND partition_id = $3",
            partitions = self.partitions_relation
        );
        let row = sqlx::query(&sql)
            .bind(lease.scope.consumer_group_id.as_str())
            .bind(lease.scope.stream_id.as_str())
            .bind(partition_id_i64)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        if let Some(r) = row {
            let stored: Option<i64> = r.get("partition_count");
            if let Some(stored) = stored
                && stored != partition_count_i64
            {
                return Err(Error::Config(format!(
                    "partition count mismatch for scope {} stream {} partition {}: stored {}, requested {}",
                    lease.scope.consumer_group_id.as_str(),
                    lease.scope.stream_id.as_str(),
                    lease.partition.id(),
                    stored,
                    partition_count_i64,
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod schema_tests {
    use super::*;

    #[test]
    fn schema_sql_contains_expected_tables() {
        let sql = PgPartitionCoordinator::schema_sql(&PgPartitionCoordinatorConfig::default());
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"event_stream_consumers\""));
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"event_stream_partitions\""));
    }
}
