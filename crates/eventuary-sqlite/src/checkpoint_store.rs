use std::sync::Arc;

use serde::{Serialize, de::DeserializeOwned};

use eventuary_core::io::CursorId;
use eventuary_core::io::reader::{CheckpointKey, CheckpointScope, CheckpointStore};
use eventuary_core::{Error, Result};

use crate::database::SqliteConn;
use crate::relation::SqliteRelationName;

#[derive(Debug, Clone)]
pub struct SqliteCheckpointStoreConfig {
    pub offsets_relation: SqliteRelationName,
}

impl Default for SqliteCheckpointStoreConfig {
    fn default() -> Self {
        Self {
            offsets_relation: SqliteRelationName::new("consumer_offsets")
                .expect("default offsets relation"),
        }
    }
}

pub struct SqliteCheckpointStore<C> {
    conn: SqliteConn,
    relation: Arc<String>,
    _cursor: std::marker::PhantomData<fn() -> C>,
}

impl<C> Clone for SqliteCheckpointStore<C> {
    fn clone(&self) -> Self {
        Self {
            conn: Arc::clone(&self.conn),
            relation: Arc::clone(&self.relation),
            _cursor: std::marker::PhantomData,
        }
    }
}

impl<C> SqliteCheckpointStore<C> {
    pub fn new(conn: SqliteConn, config: SqliteCheckpointStoreConfig) -> Self {
        Self {
            conn,
            relation: Arc::new(config.offsets_relation.render()),
            _cursor: std::marker::PhantomData,
        }
    }
}

fn encode_cursor_id(cursor_id: &CursorId) -> String {
    match cursor_id {
        CursorId::Global => "global".to_owned(),
        CursorId::Named(id) => id.to_string(),
    }
}

fn decode_cursor_id(value: &str) -> CursorId {
    if value == "global" {
        CursorId::Global
    } else {
        CursorId::Named(Arc::from(value))
    }
}

fn encode_cursor<C: Serialize>(cursor: &C) -> Result<String> {
    serde_json::to_string(cursor)
        .map_err(|e| Error::Serialization(format!("checkpoint encode: {e}")))
}

fn decode_cursor<C: DeserializeOwned>(value: String) -> Result<C> {
    serde_json::from_str(&value)
        .map_err(|e| Error::Serialization(format!("checkpoint decode: {e}")))
}

impl<C> CheckpointStore<C> for SqliteCheckpointStore<C>
where
    C: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    async fn load(&self, key: &CheckpointKey) -> Result<Option<C>> {
        let conn = Arc::clone(&self.conn);
        let relation = Arc::clone(&self.relation);
        let cursor_id = encode_cursor_id(&key.cursor_id);
        let group = key.scope.consumer_group_id.as_str().to_owned();
        let stream = key.scope.stream_id.as_str().to_owned();
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            let sql = format!(
                "SELECT cursor FROM {relation} \
                 WHERE consumer_group_id = ?1 \
                   AND stream_id = ?2 \
                   AND cursor_id = ?3"
            );
            let row = guard
                .query_row(&sql, rusqlite::params![group, stream, cursor_id], |r| {
                    r.get::<_, String>(0)
                })
                .map(Some)
                .or_else(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => Ok(None),
                    other => Err(other),
                })
                .map_err(|e| Error::Store(e.to_string()))?;
            match row {
                Some(json) => Ok(Some(decode_cursor::<C>(json)?)),
                None => Ok(None),
            }
        })
        .await
        .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
    }

    async fn load_scope(&self, scope: &CheckpointScope) -> Result<Vec<(CursorId, C)>> {
        let conn = Arc::clone(&self.conn);
        let relation = Arc::clone(&self.relation);
        let group = scope.consumer_group_id.as_str().to_owned();
        let stream = scope.stream_id.as_str().to_owned();
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            let sql = format!(
                "SELECT cursor_id, cursor FROM {relation} \
                 WHERE consumer_group_id = ?1 AND stream_id = ?2"
            );
            let mut stmt = guard
                .prepare(&sql)
                .map_err(|e| Error::Store(e.to_string()))?;
            let rows = stmt
                .query_map(rusqlite::params![group, stream], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
                })
                .map_err(|e| Error::Store(e.to_string()))?;
            let mut out = Vec::new();
            for row in rows {
                let (cursor_id_str, json) = row.map_err(|e| Error::Store(e.to_string()))?;
                out.push((decode_cursor_id(&cursor_id_str), decode_cursor::<C>(json)?));
            }
            Ok(out)
        })
        .await
        .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
    }

    async fn commit(&self, key: &CheckpointKey, cursor: C) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        let relation = Arc::clone(&self.relation);
        let cursor_id = encode_cursor_id(&key.cursor_id);
        let group = key.scope.consumer_group_id.as_str().to_owned();
        let stream = key.scope.stream_id.as_str().to_owned();
        let cursor_json = encode_cursor(&cursor)?;
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            let sql = format!(
                "INSERT INTO {relation} (consumer_group_id, stream_id, cursor_id, cursor) \
                 VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT (consumer_group_id, stream_id, cursor_id) \
                 DO UPDATE SET cursor = excluded.cursor"
            );
            guard
                .execute(
                    &sql,
                    rusqlite::params![group, stream, cursor_id, cursor_json],
                )
                .map_err(|e| Error::Store(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventuary_core::LogicalPartition;
    use std::num::NonZeroU16;

    #[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
    struct WrappedCursor {
        sequence: i64,
        partition: LogicalPartition,
    }

    #[test]
    fn encode_cursor_preserves_nested_json() {
        let partition = LogicalPartition::new(2, NonZeroU16::new(4).unwrap()).unwrap();
        let cursor = WrappedCursor {
            sequence: 42,
            partition,
        };

        let value = encode_cursor(&cursor).unwrap();
        let decoded: WrappedCursor = decode_cursor(value).unwrap();

        assert_eq!(decoded, cursor);
    }

    #[test]
    fn cursor_id_global_encodes_as_plain_string() {
        assert_eq!(encode_cursor_id(&CursorId::Global), "global");
        assert_eq!(decode_cursor_id("global"), CursorId::Global);
    }

    #[test]
    fn cursor_id_named_roundtrips_unquoted() {
        let id = CursorId::Named(Arc::from("partition:100:17"));
        let encoded = encode_cursor_id(&id);
        assert_eq!(encoded, "partition:100:17");
        assert_eq!(decode_cursor_id(&encoded), id);
    }
}
