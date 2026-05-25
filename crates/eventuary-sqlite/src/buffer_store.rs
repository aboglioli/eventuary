//! SQLite [`BufferStore`] implementation.
//!
//! Persists buffered events plus their cursor as JSON TEXT. The
//! generic cursor `C` must round-trip via `serde_json`. `pending`
//! returns the current snapshot ordered by id; `nack` is a no-op. All
//! SQLite work runs in `spawn_blocking`.

use std::marker::PhantomData;
use std::sync::Arc;

use serde::{Serialize, de::DeserializeOwned};

use eventuary_core::io::reader::{BufferEntry, BufferStore};
use eventuary_core::{Error, Event, Result, SerializedEvent};

use crate::database::SqliteConn;
use crate::relation::SqliteRelationName;
use crate::schema::{Migration, RelationReplacement};

const BUFFER_STORE_0001_INIT_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS {buffer_entries} (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    event     TEXT    NOT NULL,
    cursor    TEXT    NOT NULL,
    pushed_at TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_buffer_entries_pushed_at ON {buffer_entries} (pushed_at);
"#;

const BUFFER_STORE_MIGRATIONS: &[Migration] = &[Migration {
    name: "0001_init",
    sql: BUFFER_STORE_0001_INIT_SQL,
}];

#[derive(Debug, Clone)]
pub struct SqliteBufferStoreConfig {
    pub relation: SqliteRelationName,
}

impl Default for SqliteBufferStoreConfig {
    fn default() -> Self {
        Self {
            relation: SqliteRelationName::new("buffer_entries").expect("default buffer relation"),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct SqliteBufferStoreId(i64);

impl SqliteBufferStoreId {
    pub fn as_i64(&self) -> i64 {
        self.0
    }
}

pub struct SqliteBufferStore<C> {
    conn: SqliteConn,
    relation: Arc<String>,
    _cursor: PhantomData<C>,
}

impl<C> Clone for SqliteBufferStore<C> {
    fn clone(&self) -> Self {
        Self {
            conn: Arc::clone(&self.conn),
            relation: Arc::clone(&self.relation),
            _cursor: PhantomData,
        }
    }
}

impl<C> SqliteBufferStore<C> {
    pub fn new(conn: SqliteConn, config: SqliteBufferStoreConfig) -> Self {
        Self {
            conn,
            relation: Arc::new(config.relation.render()),
            _cursor: PhantomData,
        }
    }

    pub fn connect(conn: SqliteConn, config: SqliteBufferStoreConfig) -> Result<Self> {
        Self::prepare_schema(&conn, &config)?;
        Ok(Self::new(conn, config))
    }

    pub fn prepare_schema(conn: &SqliteConn, config: &SqliteBufferStoreConfig) -> Result<()> {
        let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
        crate::schema::apply_schema(
            &guard,
            BUFFER_STORE_MIGRATIONS,
            &[RelationReplacement {
                token: "{buffer_entries}",
                relation: &config.relation,
            }],
        )
    }

    pub fn schema_sql(config: &SqliteBufferStoreConfig) -> String {
        crate::schema::render_schema_sql(
            BUFFER_STORE_MIGRATIONS,
            &[RelationReplacement {
                token: "{buffer_entries}",
                relation: &config.relation,
            }],
        )
    }
}

fn encode_cursor<C: Serialize>(cursor: &C) -> Result<String> {
    serde_json::to_string(cursor).map_err(|e| Error::Serialization(format!("buffer encode: {e}")))
}

fn decode_cursor<C: DeserializeOwned>(value: &str) -> Result<C> {
    serde_json::from_str(value)
        .map_err(|e| Error::Serialization(format!("buffer decode cursor: {e}")))
}

fn encode_event(event: &Event) -> Result<String> {
    let serialized = SerializedEvent::from_event(event)?;
    serde_json::to_string(&serialized)
        .map_err(|e| Error::Serialization(format!("buffer encode event: {e}")))
}

fn decode_event(value: &str) -> Result<Event> {
    let serialized: SerializedEvent = serde_json::from_str(value)
        .map_err(|e| Error::Serialization(format!("buffer decode event: {e}")))?;
    serialized.to_event()
}

impl<C> BufferStore<C> for SqliteBufferStore<C>
where
    C: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    type Id = SqliteBufferStoreId;

    async fn push(&self, event: &Event, cursor: &C) -> Result<Self::Id> {
        let conn = Arc::clone(&self.conn);
        let relation = Arc::clone(&self.relation);
        let event_json = encode_event(event)?;
        let cursor_json = encode_cursor(cursor)?;
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            let sql =
                format!("INSERT INTO {relation} (event, cursor) VALUES (?1, ?2) RETURNING id");
            let id: i64 = guard
                .query_row(&sql, rusqlite::params![event_json, cursor_json], |r| {
                    r.get(0)
                })
                .map_err(|e| Error::Store(e.to_string()))?;
            Ok(SqliteBufferStoreId(id))
        })
        .await
        .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
    }

    async fn pending(&self) -> Result<Vec<BufferEntry<C, Self::Id>>> {
        let conn = Arc::clone(&self.conn);
        let relation = Arc::clone(&self.relation);
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            let sql = format!("SELECT id, event, cursor FROM {relation} ORDER BY id");
            let mut stmt = guard
                .prepare(&sql)
                .map_err(|e| Error::Store(e.to_string()))?;
            let rows = stmt
                .query_map([], |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                    ))
                })
                .map_err(|e| Error::Store(e.to_string()))?;
            let mut out = Vec::new();
            for row in rows {
                let (id, event_json, cursor_json) = row.map_err(|e| Error::Store(e.to_string()))?;
                out.push(BufferEntry {
                    id: SqliteBufferStoreId(id),
                    event: decode_event(&event_json)?,
                    cursor: decode_cursor::<C>(&cursor_json)?,
                });
            }
            Ok(out)
        })
        .await
        .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
    }

    async fn ack(&self, id: &Self::Id) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        let relation = Arc::clone(&self.relation);
        let id_value = id.0;
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            let sql = format!("DELETE FROM {relation} WHERE id = ?1");
            guard
                .execute(&sql, rusqlite::params![id_value])
                .map_err(|e| Error::Store(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
    }

    async fn nack(&self, _id: &Self::Id) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod schema_tests {
    use super::*;

    #[test]
    fn schema_sql_contains_expected_table() {
        let sql = SqliteBufferStore::<crate::reader::SqliteCursor>::schema_sql(
            &SqliteBufferStoreConfig::default(),
        );
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"buffer_entries\""));
    }
}
