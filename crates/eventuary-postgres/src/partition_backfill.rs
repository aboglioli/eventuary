use std::collections::HashMap;
use std::fmt;
use std::num::NonZeroU16;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};

use eventuary_core::partition::{PartitionHasher, PartitionKeyResolver, PartitionStrategy};
use eventuary_core::{Error, Result, SerializedEvent, SerializedPayload};

use crate::relation::PgRelationName;

pub struct PgPartitionBackfillConfig {
    pub events_relation: PgRelationName,
    pub partition_count: NonZeroU16,
    pub key_resolver: Arc<dyn PartitionKeyResolver>,
    pub hasher: Arc<dyn PartitionHasher>,
    pub batch_size: usize,
}

impl PgPartitionBackfillConfig {
    pub fn new(
        events_relation: PgRelationName,
        partition_count: NonZeroU16,
        key_resolver: impl PartitionKeyResolver + 'static,
        hasher: impl PartitionHasher + 'static,
        batch_size: usize,
    ) -> Result<Self> {
        if batch_size == 0 {
            return Err(Error::Config(
                "partition backfill batch size must be greater than zero".to_owned(),
            ));
        }

        Ok(Self {
            events_relation,
            partition_count,
            key_resolver: Arc::new(key_resolver),
            hasher: Arc::new(hasher),
            batch_size,
        })
    }
}

impl fmt::Debug for PgPartitionBackfillConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PgPartitionBackfillConfig")
            .field("events_relation", &self.events_relation)
            .field("partition_count", &self.partition_count)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

#[derive(Debug, Clone, Default)]
pub struct BackfillReport {
    pub rows_updated: u64,
    pub batches: u32,
}

pub struct PgPartitionBackfill {
    pool: PgPool,
    config: PgPartitionBackfillConfig,
}

impl PgPartitionBackfill {
    pub fn new(pool: PgPool, config: PgPartitionBackfillConfig) -> Self {
        Self { pool, config }
    }

    pub async fn run(&self) -> Result<BackfillReport> {
        let events = self.config.events_relation.render();
        let batch_size = self.config.batch_size;
        let mut report = BackfillReport::default();

        let fetch_sql = format!(
            "SELECT sequence, id::text AS id_text, organization, namespace, topic, event_key, \
             payload::text AS payload_text, content_type, metadata::text AS metadata_text, \
             timestamp::text AS timestamp_text, version, parent_id::text AS parent_id_text, \
             correlation_id, causation_id \
             FROM {events} \
             WHERE partition_id IS NULL \
             ORDER BY sequence \
             LIMIT $1",
        );

        let update_sql = format!(
            "UPDATE {events} \
             SET partition_key = $1, \
                 partition_hash = $2, \
                 partition_id = $3, \
                 partition_count = $4, \
                 partition_strategy = $5 \
             WHERE sequence = $6 AND partition_id IS NULL",
        );

        loop {
            let rows = sqlx::query(&fetch_sql)
                .bind(batch_size as i64)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| Error::Store(e.to_string()))?;

            if rows.is_empty() {
                break;
            }

            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|e| Error::Store(e.to_string()))?;

            for row in &rows {
                let sequence: i64 = row.get("sequence");
                let serialized = deserialize_row(row, sequence)?;
                let event = serialized.to_event()?;

                let partition_key = self.config.key_resolver.partition_key(&event)?;
                let partition_hash = self.config.hasher.hash(&partition_key);
                let partition = self
                    .config
                    .hasher
                    .partition_for(&partition_key, self.config.partition_count);
                let partition_strategy = PartitionStrategy::new(self.config.hasher.strategy())?;

                let result = sqlx::query(&update_sql)
                    .bind(partition_key.as_str())
                    .bind(partition_hash.to_sql_i64())
                    .bind(partition.id() as i32)
                    .bind(partition.count() as i32)
                    .bind(partition_strategy.as_str())
                    .bind(sequence)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| Error::Store(e.to_string()))?;

                report.rows_updated += result.rows_affected();
            }

            tx.commit().await.map_err(|e| Error::Store(e.to_string()))?;
            report.batches += 1;
        }

        Ok(report)
    }
}

fn deserialize_row(row: &sqlx::postgres::PgRow, sequence: i64) -> Result<SerializedEvent> {
    let id_text: String = row.get("id_text");
    let id = uuid::Uuid::parse_str(&id_text)
        .map_err(|e| Error::Serialization(format!("decode id: {e}")))?;
    let parent_id = row
        .get::<Option<String>, _>("parent_id_text")
        .as_deref()
        .map(uuid::Uuid::parse_str)
        .transpose()
        .map_err(|e| Error::Serialization(format!("decode parent_id: {e}")))?;
    let payload_str: String = row.get("payload_text");
    let payload: SerializedPayload = serde_json::from_str(&payload_str)
        .map_err(|e| Error::Serialization(format!("decode payload: {e}")))?;
    let metadata_str: String = row.get("metadata_text");
    let metadata: HashMap<String, String> = serde_json::from_str(&metadata_str)
        .map_err(|e| Error::Serialization(format!("decode metadata: {e}")))?;
    let timestamp_str: String = row.get("timestamp_text");
    let timestamp = parse_pg_timestamp(&timestamp_str).map_err(|e| {
        Error::Serialization(format!("decode timestamp at sequence {sequence}: {e}"))
    })?;
    Ok(SerializedEvent {
        id,
        organization: row.get("organization"),
        namespace: row.get("namespace"),
        topic: row.get("topic"),
        payload,
        metadata,
        timestamp,
        version: row.get::<i64, _>("version") as u64,
        key: row.get("event_key"),
        parent_id,
        correlation_id: row.get("correlation_id"),
        causation_id: row.get("causation_id"),
    })
}

fn parse_pg_timestamp(s: &str) -> std::result::Result<DateTime<Utc>, chrono::ParseError> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc));
    }
    DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%#z").map(|dt| dt.with_timezone(&Utc))
}
