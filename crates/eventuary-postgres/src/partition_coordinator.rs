use std::sync::Arc;

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};

use eventuary_core::Partition;
use eventuary_core::io::reader::CheckpointScope;
use eventuary_core::io::{Generation, OwnerId, PartitionCoordinator, PartitionLease};
use eventuary_core::{Error, Result};

use crate::reader::PgCursor;
use crate::relation::PgRelationName;

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

    async fn claim<'a>(
        &'a self,
        scope: &'a CheckpointScope,
        owner_id: &'a OwnerId,
        partition: Partition,
        lease_duration: std::time::Duration,
    ) -> Result<Option<PartitionLease<PgCursor>>> {
        let lease_until = lease_until_to_sql(compute_lease_until(lease_duration)?);
        let partition_id_i32 = partition.id() as i32;
        let sql = format!(
            "INSERT INTO {partitions} \
                (consumer_group_id, stream_id, partition_id, owner_id, lease_until, generation, checkpoint_sequence) \
             VALUES ($1, $2, $3, $4, $5::timestamptz, 1, 0) \
             ON CONFLICT (consumer_group_id, stream_id, partition_id) DO UPDATE \
             SET owner_id = EXCLUDED.owner_id, \
                 lease_until = EXCLUDED.lease_until, \
                 generation = {partitions}.generation + 1 \
             WHERE {partitions}.owner_id IS NULL \
                OR {partitions}.lease_until IS NULL \
                OR {partitions}.lease_until < NOW() \
                OR {partitions}.owner_id = EXCLUDED.owner_id \
             RETURNING owner_id, \
                       to_char(lease_until AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') AS lease_until_text, \
                       generation, \
                       checkpoint_sequence",
            partitions = self.partitions_relation
        );
        let row = sqlx::query(&sql)
            .bind(scope.consumer_group_id.as_str())
            .bind(scope.stream_id.as_str())
            .bind(partition_id_i32)
            .bind(owner_id.as_str())
            .bind(lease_until)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        match row {
            None => Ok(None),
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
                    checkpoint_cursor: Some(PgCursor::new(checkpoint_sequence)),
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
        let partition_id_i32 = lease.partition.id() as i32;
        let generation = lease.generation.get();
        let sql = format!(
            "UPDATE {partitions} \
             SET lease_until = $5::timestamptz \
             WHERE consumer_group_id = $1 \
               AND stream_id = $2 \
               AND partition_id = $3 \
               AND owner_id = $4 \
               AND generation = $6",
            partitions = self.partitions_relation
        );
        let result = sqlx::query(&sql)
            .bind(lease.scope.consumer_group_id.as_str())
            .bind(lease.scope.stream_id.as_str())
            .bind(partition_id_i32)
            .bind(lease.owner_id.as_str())
            .bind(lease_until)
            .bind(generation)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        if result.rows_affected() == 0 {
            return Err(Error::OwnershipLost(format!(
                "partition {} generation {}",
                lease.partition.id(),
                lease.generation,
            )));
        }
        Ok(())
    }

    async fn release<'a>(&'a self, lease: &'a PartitionLease<PgCursor>) -> Result<()> {
        let partition_id_i32 = lease.partition.id() as i32;
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
               AND generation = $5",
            partitions = self.partitions_relation
        );
        let result = sqlx::query(&sql)
            .bind(lease.scope.consumer_group_id.as_str())
            .bind(lease.scope.stream_id.as_str())
            .bind(partition_id_i32)
            .bind(lease.owner_id.as_str())
            .bind(generation)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        if result.rows_affected() == 0 {
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
        let partition_id_i32 = lease.partition.id() as i32;
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
               AND $6 > {partitions}.checkpoint_sequence",
            partitions = self.partitions_relation
        );
        let result = sqlx::query(&sql)
            .bind(lease.scope.consumer_group_id.as_str())
            .bind(lease.scope.stream_id.as_str())
            .bind(partition_id_i32)
            .bind(lease.owner_id.as_str())
            .bind(generation)
            .bind(sequence)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        if result.rows_affected() == 0 {
            let check_sql = format!(
                "SELECT generation, owner_id FROM {partitions} \
                 WHERE consumer_group_id = $1 AND stream_id = $2 AND partition_id = $3",
                partitions = self.partitions_relation
            );
            let check_row = sqlx::query(&check_sql)
                .bind(lease.scope.consumer_group_id.as_str())
                .bind(lease.scope.stream_id.as_str())
                .bind(partition_id_i32)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| Error::Store(e.to_string()))?;
            match check_row {
                Some(r) => {
                    let current_generation: i64 = r.get("generation");
                    let current_owner: Option<String> = r.get("owner_id");
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
