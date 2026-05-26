use std::collections::HashMap;
use std::fmt;
use std::num::NonZeroU32;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use rusqlite::types::Value;

use eventuary_core::partition::{PartitionHasher, PartitionKeyResolver, PartitionStrategy};
use eventuary_core::{Error, Result, SerializedEvent, SerializedPayload};

use crate::database::SqliteConn;
use crate::event_log::{SqliteEventLogSchema, SqliteEventLogSchemaConfig};
use crate::relation::SqliteRelationName;

pub struct SqlitePartitionBackfillConfig {
    pub events_relation: SqliteRelationName,
    pub partition_count: NonZeroU32,
    pub key_resolver: Arc<dyn PartitionKeyResolver>,
    pub hasher: Arc<dyn PartitionHasher>,
    pub batch_size: usize,
}

impl SqlitePartitionBackfillConfig {
    pub fn new(
        events_relation: SqliteRelationName,
        partition_count: NonZeroU32,
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

impl fmt::Debug for SqlitePartitionBackfillConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqlitePartitionBackfillConfig")
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

pub struct SqlitePartitionBackfill {
    conn: SqliteConn,
    config: SqlitePartitionBackfillConfig,
}

impl SqlitePartitionBackfill {
    pub fn new(conn: SqliteConn, config: SqlitePartitionBackfillConfig) -> Self {
        Self { conn, config }
    }

    pub fn connect(conn: SqliteConn, config: SqlitePartitionBackfillConfig) -> Result<Self> {
        Self::prepare_schema(&conn, &config)?;
        Ok(Self::new(conn, config))
    }

    pub fn prepare_schema(conn: &SqliteConn, config: &SqlitePartitionBackfillConfig) -> Result<()> {
        let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
        SqliteEventLogSchema::prepare(
            &guard,
            &SqliteEventLogSchemaConfig {
                events_relation: config.events_relation.clone(),
            },
        )
    }

    pub fn schema_sql(config: &SqlitePartitionBackfillConfig) -> String {
        SqliteEventLogSchema::schema_sql(&SqliteEventLogSchemaConfig {
            events_relation: config.events_relation.clone(),
        })
    }

    pub async fn run(&self) -> Result<BackfillReport> {
        let events = self.config.events_relation.render();
        let batch_size = self.config.batch_size;
        let partition_count = self.config.partition_count;
        let key_resolver = Arc::clone(&self.config.key_resolver);
        let hasher = Arc::clone(&self.config.hasher);
        let conn = Arc::clone(&self.conn);

        let fetch_sql = format!(
            "SELECT sequence, id, organization, namespace, topic, event_key, payload, \
             content_type, metadata, timestamp, version, parent_id, correlation_id, causation_id \
             FROM {events} \
             WHERE partition_id IS NULL \
             ORDER BY sequence \
             LIMIT ?1"
        );

        let update_sql = format!(
            "UPDATE {events} \
             SET partition_key = ?1, \
                 partition_hash = ?2, \
                 partition_id = ?3, \
                 partition_count = ?4, \
                 partition_strategy = ?5 \
             WHERE sequence = ?6 AND partition_id IS NULL"
        );

        tokio::task::spawn_blocking(move || {
            let mut report = BackfillReport::default();
            let mut guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;

            loop {
                let rows = {
                    let mut stmt = guard
                        .prepare(&fetch_sql)
                        .map_err(|e| Error::Store(e.to_string()))?;
                    let mapped = stmt
                        .query_map([Value::Integer(batch_size as i64)], decode_row)
                        .map_err(|e| Error::Store(e.to_string()))?;
                    let mut out: Vec<(i64, SerializedEvent)> = Vec::new();
                    for row in mapped {
                        out.push(row.map_err(|e| Error::Store(e.to_string()))?);
                    }
                    out
                };

                if rows.is_empty() {
                    break;
                }

                let tx = guard
                    .transaction()
                    .map_err(|e| Error::Store(e.to_string()))?;

                for (sequence, serialized) in &rows {
                    let event = serialized.to_event()?;
                    let partition_key = key_resolver.partition_key(&event)?;
                    let partition_hash = hasher.hash(&partition_key);
                    let partition = hasher.partition_for(&partition_key, partition_count);
                    let partition_strategy = PartitionStrategy::new(hasher.strategy())?;

                    let updated = tx
                        .execute(
                            &update_sql,
                            rusqlite::params![
                                partition_key.as_str(),
                                partition_hash.to_sql_i64(),
                                partition.id() as i64,
                                partition.count() as i64,
                                partition_strategy.as_str(),
                                *sequence,
                            ],
                        )
                        .map_err(|e| Error::Store(e.to_string()))?;
                    report.rows_updated += updated as u64;
                }

                tx.commit().map_err(|e| Error::Store(e.to_string()))?;
                report.batches += 1;
            }

            Ok(report)
        })
        .await
        .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
    }
}

fn decode_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<(i64, SerializedEvent)> {
    let sequence: i64 = row.get(0)?;
    let id: String = row.get(1)?;
    let organization: String = row.get(2)?;
    let namespace: String = row.get(3)?;
    let topic: String = row.get(4)?;
    let key: String = row.get(5)?;
    let payload_str: String = row.get(6)?;
    let _content_type: String = row.get(7)?;
    let metadata_str: String = row.get(8)?;
    let timestamp_str: String = row.get(9)?;
    let version: i64 = row.get(10)?;
    let parent_id: Option<String> = row.get(11)?;
    let correlation_id: Option<String> = row.get(12)?;
    let causation_id: Option<String> = row.get(13)?;

    let id = uuid::Uuid::parse_str(&id).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(
            1,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::other(format!("decode id: {e}"))),
        )
    })?;
    let parent_id = parent_id
        .as_deref()
        .map(uuid::Uuid::parse_str)
        .transpose()
        .map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                11,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::other(format!("decode parent_id: {e}"))),
            )
        })?;
    let payload: SerializedPayload = serde_json::from_str(&payload_str).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(
            6,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::other(format!("decode payload: {e}"))),
        )
    })?;
    let metadata: HashMap<String, String> = serde_json::from_str(&metadata_str).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(
            8,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::other(format!("decode metadata: {e}"))),
        )
    })?;
    let timestamp = DateTime::parse_from_rfc3339(&timestamp_str)
        .map(|d| d.with_timezone(&Utc))
        .map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                9,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::other(format!("decode timestamp: {e}"))),
            )
        })?;

    Ok((
        sequence,
        SerializedEvent {
            id,
            organization,
            namespace,
            topic,
            payload,
            metadata,
            timestamp,
            version: version as u64,
            key,
            parent_id,
            correlation_id,
            causation_id,
        },
    ))
}
