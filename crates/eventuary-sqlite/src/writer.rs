use std::sync::Arc;

use eventuary_core::io::Writer;
use eventuary_core::{Error, Event, Result, SerializedEvent};

use crate::database::SqliteConn;
use crate::relation::SqliteRelationName;

#[derive(Debug, Clone)]
pub struct SqliteWriterConfig {
    pub events_relation: SqliteRelationName,
}

impl Default for SqliteWriterConfig {
    fn default() -> Self {
        Self {
            events_relation: SqliteRelationName::new("events").expect("default events relation"),
        }
    }
}

pub struct SqliteWriter {
    conn: SqliteConn,
    insert_sql: Arc<String>,
}

impl SqliteWriter {
    pub fn new(conn: SqliteConn) -> Self {
        Self::new_with_config(conn, SqliteWriterConfig::default())
    }

    pub fn new_with_config(conn: SqliteConn, config: SqliteWriterConfig) -> Self {
        let insert_sql = format!(
            "INSERT INTO {events} (id, organization, namespace, topic, event_key, payload, content_type, metadata, timestamp, version, parent_id, correlation_id, causation_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            events = config.events_relation.render(),
        );
        Self {
            conn,
            insert_sql: Arc::new(insert_sql),
        }
    }
}

impl Writer for SqliteWriter {
    async fn write(&self, event: &Event) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        let event = event.clone();
        let sql = Arc::clone(&self.insert_sql);
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            insert_event(&guard, &sql, &event)
        })
        .await
        .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
    }

    async fn write_all(&self, events: &[Event]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let conn = Arc::clone(&self.conn);
        let events = events.to_vec();
        let sql = Arc::clone(&self.insert_sql);
        tokio::task::spawn_blocking(move || {
            let mut guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            let tx = guard
                .transaction()
                .map_err(|e| Error::Store(e.to_string()))?;
            for event in &events {
                insert_event(&tx, &sql, event)?;
            }
            tx.commit().map_err(|e| Error::Store(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
    }
}

fn insert_event(conn: &rusqlite::Connection, sql: &str, event: &Event) -> Result<()> {
    let serialized = SerializedEvent::from_event(event)?;
    let content_type = serialized.payload.content_type().to_string();
    let payload = serde_json::to_string(&serialized.payload)
        .map_err(|e| Error::Store(format!("encode payload: {e}")))?;
    let metadata = serde_json::to_string(&serialized.metadata)
        .map_err(|e| Error::Store(format!("encode metadata: {e}")))?;
    conn.execute(
        sql,
        rusqlite::params![
            serialized.id,
            serialized.organization,
            serialized.namespace,
            serialized.topic,
            serialized.key,
            payload,
            content_type,
            metadata,
            serialized.timestamp.to_rfc3339(),
            serialized.version as i64,
            serialized.parent_id,
            serialized.correlation_id,
            serialized.causation_id,
        ],
    )
    .map_err(|e| Error::Store(e.to_string()))?;
    Ok(())
}
