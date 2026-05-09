use std::sync::Arc;

use eventuary::io::Writer;
use eventuary::{Error, Event, Result, SerializedEvent};

use crate::database::SqliteConn;

pub struct SqliteEventWriter {
    conn: SqliteConn,
}

impl SqliteEventWriter {
    pub fn new(conn: SqliteConn) -> Self {
        Self { conn }
    }
}

impl Writer for SqliteEventWriter {
    async fn write(&self, event: &Event) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        let event = event.clone();
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            insert_event(&guard, &event)
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
        tokio::task::spawn_blocking(move || {
            let mut guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            let tx = guard
                .transaction()
                .map_err(|e| Error::Store(e.to_string()))?;
            for event in &events {
                insert_event(&tx, event)?;
            }
            tx.commit().map_err(|e| Error::Store(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
    }
}

fn insert_event(conn: &rusqlite::Connection, event: &Event) -> Result<()> {
    let serialized = SerializedEvent::from_event(event)?;
    let payload = serde_json::to_string(&serialized.payload)
        .map_err(|e| Error::Store(format!("encode payload: {e}")))?;
    let metadata = serde_json::to_string(&serialized.metadata)
        .map_err(|e| Error::Store(format!("encode metadata: {e}")))?;
    conn.execute(
        "INSERT INTO events (id, organization, namespace, topic, event_key, payload, content_type, metadata, timestamp, version)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        rusqlite::params![
            serialized.id,
            serialized.organization,
            serialized.namespace,
            serialized.topic,
            serialized.key,
            payload,
            serialized.content_type,
            metadata,
            serialized.timestamp.to_rfc3339(),
            serialized.version as i64,
        ],
    )
    .map_err(|e| Error::Store(e.to_string()))?;
    Ok(())
}
