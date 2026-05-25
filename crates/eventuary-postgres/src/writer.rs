use std::fmt;
use std::num::NonZeroU16;
use std::sync::Arc;

use sqlx::PgPool;

use eventuary_core::io::Writer;
use eventuary_core::partition::{
    PartitionHash, PartitionHasher, PartitionKey, PartitionKeyResolver, PartitionStrategy,
};
use eventuary_core::{Error, Event, Result, SerializedEvent};

use crate::relation::PgRelationName;

#[derive(Clone, Default)]
pub enum PgPartitioningConfig {
    #[default]
    Off,
    Inline {
        partition_count: NonZeroU16,
        key_resolver: Arc<dyn PartitionKeyResolver>,
        hasher: Arc<dyn PartitionHasher>,
    },
}

impl PgPartitioningConfig {
    pub fn inline(
        count: NonZeroU16,
        resolver: impl PartitionKeyResolver + 'static,
        hasher: impl PartitionHasher + 'static,
    ) -> Self {
        Self::Inline {
            partition_count: count,
            key_resolver: Arc::new(resolver),
            hasher: Arc::new(hasher),
        }
    }
}

impl fmt::Debug for PgPartitioningConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Off => write!(f, "PgPartitioningConfig::Off"),
            Self::Inline {
                partition_count, ..
            } => f
                .debug_struct("PgPartitioningConfig::Inline")
                .field("partition_count", partition_count)
                .finish(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PgWriterConfig {
    pub events_relation: PgRelationName,
    pub partitioning: PgPartitioningConfig,
}

impl Default for PgWriterConfig {
    fn default() -> Self {
        Self {
            events_relation: PgRelationName::new("events").expect("default events relation"),
            partitioning: PgPartitioningConfig::Off,
        }
    }
}

pub struct PgWriter {
    pool: PgPool,
    insert_sql: String,
    partitioning: PgPartitioningConfig,
}

impl PgWriter {
    pub fn new(pool: PgPool) -> Self {
        Self::new_with_config(pool, PgWriterConfig::default())
    }

    pub fn new_with_config(pool: PgPool, config: PgWriterConfig) -> Self {
        let insert_sql = format!(
            "INSERT INTO {events} \
             (id, organization, namespace, topic, event_key, payload, content_type, metadata, \
             timestamp, version, parent_id, correlation_id, causation_id, \
             partition_key, partition_hash, partition_id, partition_count, partition_strategy) \
             VALUES \
             ($1::uuid, $2, $3, $4, $5, $6::jsonb, $7, $8::jsonb, $9::timestamptz, $10, \
             $11::uuid, $12, $13, $14, $15, $16, $17, $18)",
            events = config.events_relation.render(),
        );
        Self {
            pool,
            insert_sql,
            partitioning: config.partitioning,
        }
    }

    fn partition_data(&self, event: &Event) -> Result<PartitionData> {
        match &self.partitioning {
            PgPartitioningConfig::Off => Ok(PartitionData::default()),
            PgPartitioningConfig::Inline {
                partition_count,
                key_resolver,
                hasher,
            } => {
                let partition_key = key_resolver.partition_key(event)?;
                let partition_hash = hasher.hash(&partition_key);
                let partition = hasher.partition_for(&partition_key, *partition_count);
                let partition_strategy = PartitionStrategy::new(hasher.strategy())?;
                Ok(PartitionData {
                    partition_key: Some(partition_key),
                    partition_hash: Some(partition_hash),
                    partition_id: Some(partition.id() as i32),
                    partition_count: Some(partition.count() as i32),
                    partition_strategy: Some(partition_strategy),
                })
            }
        }
    }
}

impl Writer for PgWriter {
    async fn write(&self, event: &Event) -> Result<()> {
        let row = EventRow::from_event(event)?;
        let pd = self.partition_data(event)?;

        sqlx::query(&self.insert_sql)
            .bind(&row.id)
            .bind(&row.organization)
            .bind(&row.namespace)
            .bind(&row.topic)
            .bind(&row.key)
            .bind(&row.payload)
            .bind(&row.content_type)
            .bind(&row.metadata)
            .bind(&row.timestamp)
            .bind(row.version)
            .bind(&row.parent_id)
            .bind(&row.correlation_id)
            .bind(&row.causation_id)
            .bind(pd.partition_key.as_ref().map(|k| k.as_str()))
            .bind(pd.partition_hash.map(|h| h.to_sql_i64()))
            .bind(pd.partition_id)
            .bind(pd.partition_count)
            .bind(pd.partition_strategy.as_ref().map(|s| s.as_str()))
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;

        Ok(())
    }

    async fn write_all(&self, events: &[Event]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        for event in events {
            let row = EventRow::from_event(event)?;
            let pd = self.partition_data(event)?;
            sqlx::query(&self.insert_sql)
                .bind(&row.id)
                .bind(&row.organization)
                .bind(&row.namespace)
                .bind(&row.topic)
                .bind(&row.key)
                .bind(&row.payload)
                .bind(&row.content_type)
                .bind(&row.metadata)
                .bind(&row.timestamp)
                .bind(row.version)
                .bind(&row.parent_id)
                .bind(&row.correlation_id)
                .bind(&row.causation_id)
                .bind(pd.partition_key.as_ref().map(|k| k.as_str()))
                .bind(pd.partition_hash.map(|h| h.to_sql_i64()))
                .bind(pd.partition_id)
                .bind(pd.partition_count)
                .bind(pd.partition_strategy.as_ref().map(|s| s.as_str()))
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Store(e.to_string()))?;
        }
        tx.commit().await.map_err(|e| Error::Store(e.to_string()))?;
        Ok(())
    }
}

#[derive(Default)]
struct PartitionData {
    partition_key: Option<PartitionKey>,
    partition_hash: Option<PartitionHash>,
    partition_id: Option<i32>,
    partition_count: Option<i32>,
    partition_strategy: Option<PartitionStrategy>,
}

struct EventRow {
    id: String,
    organization: String,
    namespace: String,
    topic: String,
    key: String,
    payload: String,
    content_type: String,
    metadata: String,
    timestamp: String,
    version: i64,
    parent_id: Option<String>,
    correlation_id: Option<String>,
    causation_id: Option<String>,
}

impl EventRow {
    fn from_event(event: &Event) -> Result<Self> {
        let serialized = SerializedEvent::from_event(event)?;
        let content_type = serialized.payload.content_type().to_string();
        let payload = serde_json::to_string(&serialized.payload)
            .map_err(|e| Error::Store(format!("encode payload: {e}")))?;
        let metadata = serde_json::to_string(&serialized.metadata)
            .map_err(|e| Error::Store(format!("encode metadata: {e}")))?;
        Ok(Self {
            id: serialized.id.to_string(),
            organization: serialized.organization,
            namespace: serialized.namespace,
            topic: serialized.topic,
            key: serialized.key,
            payload,
            content_type,
            metadata,
            timestamp: serialized.timestamp.to_rfc3339(),
            version: serialized.version as i64,
            parent_id: serialized.parent_id.map(|id| id.to_string()),
            correlation_id: serialized.correlation_id,
            causation_id: serialized.causation_id,
        })
    }
}
