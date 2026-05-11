use sqlx::PgPool;

use eventuary_core::io::Writer;
use eventuary_core::{Error, Event, Result, SerializedEvent};

pub struct PgEventWriter {
    pool: PgPool,
}

impl PgEventWriter {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl Writer for PgEventWriter {
    async fn write(&self, event: &Event) -> Result<()> {
        let row = EventRow::from_event(event)?;

        sqlx::query(
            "INSERT INTO events (id, organization, namespace, topic, event_key, payload, content_type, metadata, timestamp, version) \
             VALUES ($1::uuid, $2, $3, $4, $5, $6::jsonb, $7, $8::jsonb, $9::timestamptz, $10)",
        )
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
            sqlx::query(
                "INSERT INTO events (id, organization, namespace, topic, event_key, payload, content_type, metadata, timestamp, version) \
                 VALUES ($1::uuid, $2, $3, $4, $5, $6::jsonb, $7, $8::jsonb, $9::timestamptz, $10)",
            )
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
    key: String,
    payload: String,
    content_type: String,
    metadata: String,
    timestamp: String,
    version: i64,
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
        })
    }
}
