//! PostgreSQL [`BufferStore`] implementation.
//!
//! Persists buffered events plus their cursor as JSONB. The generic
//! cursor `C` must round-trip via `serde_json`. `pending` returns the
//! current snapshot ordered by id; `nack` is a no-op (entries remain
//! visible to the next `pending` call until acked).

use std::marker::PhantomData;
use std::sync::Arc;

use serde::{Serialize, de::DeserializeOwned};
use sqlx::{PgPool, Row};

use eventuary_core::io::reader::{BufferEntry, BufferStore};
use eventuary_core::{Error, Event, Result, SerializedEvent};

use crate::relation::PgRelationName;

#[derive(Debug, Clone)]
pub struct PgBufferStoreConfig {
    pub relation: PgRelationName,
}

impl Default for PgBufferStoreConfig {
    fn default() -> Self {
        Self {
            relation: PgRelationName::new("buffer_entries").expect("default buffer relation"),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct PgBufferStoreId(i64);

impl PgBufferStoreId {
    pub fn as_i64(&self) -> i64 {
        self.0
    }
}

pub struct PgBufferStore<C> {
    pool: PgPool,
    relation: Arc<String>,
    _cursor: PhantomData<C>,
}

impl<C> Clone for PgBufferStore<C> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            relation: Arc::clone(&self.relation),
            _cursor: PhantomData,
        }
    }
}

impl<C> PgBufferStore<C> {
    pub fn new(pool: PgPool, config: PgBufferStoreConfig) -> Self {
        Self {
            pool,
            relation: Arc::new(config.relation.render()),
            _cursor: PhantomData,
        }
    }
}

fn encode_cursor<C: Serialize>(cursor: &C) -> Result<serde_json::Value> {
    serde_json::to_value(cursor).map_err(|e| Error::Serialization(format!("buffer encode: {e}")))
}

fn decode_cursor<C: DeserializeOwned>(value: serde_json::Value) -> Result<C> {
    serde_json::from_value(value)
        .map_err(|e| Error::Serialization(format!("buffer decode cursor: {e}")))
}

fn encode_event(event: &Event) -> Result<serde_json::Value> {
    let serialized = SerializedEvent::from_event(event)?;
    serde_json::to_value(serialized)
        .map_err(|e| Error::Serialization(format!("buffer encode event: {e}")))
}

fn decode_event(value: serde_json::Value) -> Result<Event> {
    let serialized: SerializedEvent = serde_json::from_value(value)
        .map_err(|e| Error::Serialization(format!("buffer decode event: {e}")))?;
    serialized.to_event()
}

impl<C> BufferStore<C> for PgBufferStore<C>
where
    C: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    type Id = PgBufferStoreId;

    async fn push(&self, event: &Event, cursor: &C) -> Result<Self::Id> {
        let sql = format!(
            "INSERT INTO {relation} (event, cursor) VALUES ($1, $2) RETURNING id",
            relation = self.relation
        );
        let row = sqlx::query(&sql)
            .bind(encode_event(event)?)
            .bind(encode_cursor(cursor)?)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        Ok(PgBufferStoreId(row.get::<i64, _>("id")))
    }

    async fn pending(&self) -> Result<Vec<BufferEntry<C, Self::Id>>> {
        let sql = format!(
            "SELECT id, event, cursor FROM {relation} ORDER BY id",
            relation = self.relation
        );
        let rows = sqlx::query(&sql)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let id: i64 = row.get("id");
            let event_value: serde_json::Value = row.get("event");
            let cursor_value: serde_json::Value = row.get("cursor");
            out.push(BufferEntry {
                id: PgBufferStoreId(id),
                event: decode_event(event_value)?,
                cursor: decode_cursor::<C>(cursor_value)?,
            });
        }
        Ok(out)
    }

    async fn ack(&self, id: &Self::Id) -> Result<()> {
        let sql = format!(
            "DELETE FROM {relation} WHERE id = $1",
            relation = self.relation
        );
        sqlx::query(&sql)
            .bind(id.0)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        Ok(())
    }

    async fn nack(&self, _id: &Self::Id) -> Result<()> {
        Ok(())
    }
}
