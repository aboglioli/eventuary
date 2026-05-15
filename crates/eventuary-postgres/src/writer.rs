use sqlx::PgPool;

use eventuary_core::io::Writer;
use eventuary_core::{Error, Event, Result, SerializedEvent};

use crate::relation::PgRelationName;

#[derive(Debug, Clone)]
pub struct PgWriterConfig {
    pub events_relation: PgRelationName,
}

impl Default for PgWriterConfig {
    fn default() -> Self {
        Self {
            events_relation: PgRelationName::new("events").expect("default events relation"),
        }
    }
}

pub struct PgWriter {
    pool: PgPool,
    insert_sql: String,
}

impl PgWriter {
    pub fn new(pool: PgPool) -> Self {
        Self::new_with_config(pool, PgWriterConfig::default())
    }

    pub fn new_with_config(pool: PgPool, config: PgWriterConfig) -> Self {
        let insert_sql = format!(
            "INSERT INTO {events} (id, organization, namespace, topic, event_key, payload, content_type, metadata, timestamp, version, parent_id, correlation_id, causation_id) \
             VALUES ($1::uuid, $2, $3, $4, $5, $6::jsonb, $7, $8::jsonb, $9::timestamptz, $10, $11::uuid, $12, $13)",
            events = config.events_relation.render(),
        );
        Self { pool, insert_sql }
    }
}

impl Writer for PgWriter {
    async fn write(&self, event: &Event) -> Result<()> {
        let row = EventRow::from_event(event)?;

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
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Store(e.to_string()))?;
        }
        tx.commit().await.map_err(|e| Error::Store(e.to_string()))?;
        Ok(())
    }
}

struct EventRow {
    id: String,
    organization: String,
    namespace: String,
    topic: String,
    key: Option<String>,
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
        let payload = serde_json::to_string(&serialized.payload)
            .map_err(|e| Error::Store(format!("encode payload: {e}")))?;
        let metadata = serde_json::to_string(&serialized.metadata)
            .map_err(|e| Error::Store(format!("encode metadata: {e}")))?;
        Ok(Self {
            id: serialized.id,
            organization: serialized.organization,
            namespace: serialized.namespace,
            topic: serialized.topic,
            key: serialized.key,
            payload,
            content_type: serialized.content_type,
            metadata,
            timestamp: serialized.timestamp.to_rfc3339(),
            version: serialized.version as i64,
            parent_id: serialized.parent_id,
            correlation_id: serialized.correlation_id,
            causation_id: serialized.causation_id,
        })
    }
}
