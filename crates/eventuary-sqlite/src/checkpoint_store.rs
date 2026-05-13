use std::num::NonZeroU16;
use std::sync::Arc;

use serde::{Serialize, de::DeserializeOwned};

use eventuary_core::io::checkpoint::{CheckpointKey, CheckpointScope, CheckpointStore};
use eventuary_core::{Error, LogicalPartition, Result};

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

fn encode_partition(partition: Option<LogicalPartition>) -> (i32, i32) {
    match partition {
        Some(p) => (p.id() as i32, p.count() as i32),
        None => (0, 1),
    }
}

fn decode_partition(p: i32, count: i32) -> Option<LogicalPartition> {
    if p == 0 && count == 1 {
        return None;
    }
    let nz = NonZeroU16::new(count.max(1) as u16)?;
    LogicalPartition::new(p as u16, nz).ok()
}

fn encode_cursor<C: Serialize>(cursor: &C) -> Result<i64> {
    let value = serde_json::to_value(cursor)
        .map_err(|e| Error::Serialization(format!("checkpoint encode: {e}")))?;
    if let Some(n) = value.as_i64() {
        return Ok(n);
    }
    Err(Error::Serialization(
        "SqliteCheckpointStore: cursor must encode as integer".to_owned(),
    ))
}

fn decode_cursor<C: DeserializeOwned>(sequence: i64) -> Result<C> {
    let value = serde_json::Value::from(sequence);
    serde_json::from_value(value)
        .map_err(|e| Error::Serialization(format!("checkpoint decode: {e}")))
}

impl<C> CheckpointStore<C> for SqliteCheckpointStore<C>
where
    C: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    async fn load(&self, key: &CheckpointKey) -> Result<Option<C>> {
        let conn = Arc::clone(&self.conn);
        let relation = Arc::clone(&self.relation);
        let (partition, partition_count) = encode_partition(key.partition);
        let group = key.scope.consumer_group_id.as_str().to_owned();
        let stream = key.scope.stream_id.as_str().to_owned();
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            let sql = format!(
                "SELECT sequence FROM {relation} \
                 WHERE consumer_group_id = ?1 \
                   AND stream_id   = ?2 \
                   AND partition         = ?3 \
                   AND partition_count   = ?4"
            );
            let row = guard
                .query_row(
                    &sql,
                    rusqlite::params![group, stream, partition, partition_count],
                    |r| r.get::<_, i64>(0),
                )
                .map(Some)
                .or_else(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => Ok(None),
                    other => Err(other),
                })
                .map_err(|e| Error::Store(e.to_string()))?;
            match row {
                Some(seq) => Ok(Some(decode_cursor::<C>(seq)?)),
                None => Ok(None),
            }
        })
        .await
        .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
    }

    async fn load_scope(
        &self,
        scope: &CheckpointScope,
    ) -> Result<Vec<(Option<LogicalPartition>, C)>> {
        let conn = Arc::clone(&self.conn);
        let relation = Arc::clone(&self.relation);
        let group = scope.consumer_group_id.as_str().to_owned();
        let stream = scope.stream_id.as_str().to_owned();
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            let sql = format!(
                "SELECT partition, partition_count, sequence FROM {relation} \
                 WHERE consumer_group_id = ?1 AND stream_id = ?2"
            );
            let mut stmt = guard
                .prepare(&sql)
                .map_err(|e| Error::Store(e.to_string()))?;
            let rows = stmt
                .query_map(rusqlite::params![group, stream], |r| {
                    Ok((
                        r.get::<_, i32>(0)?,
                        r.get::<_, i32>(1)?,
                        r.get::<_, i64>(2)?,
                    ))
                })
                .map_err(|e| Error::Store(e.to_string()))?;
            let mut out = Vec::new();
            for row in rows {
                let (p, pc, seq) = row.map_err(|e| Error::Store(e.to_string()))?;
                out.push((decode_partition(p, pc), decode_cursor::<C>(seq)?));
            }
            Ok(out)
        })
        .await
        .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
    }

    async fn commit(&self, key: &CheckpointKey, cursor: C) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        let relation = Arc::clone(&self.relation);
        let (partition, partition_count) = encode_partition(key.partition);
        let group = key.scope.consumer_group_id.as_str().to_owned();
        let stream = key.scope.stream_id.as_str().to_owned();
        let sequence = encode_cursor(&cursor)?;
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            let sql = format!(
                "INSERT INTO {relation} (consumer_group_id, stream_id, partition, partition_count, sequence) \
                 VALUES (?1, ?2, ?3, ?4, ?5) \
                 ON CONFLICT (consumer_group_id, stream_id, partition, partition_count) \
                 DO UPDATE SET sequence = excluded.sequence \
                 WHERE excluded.sequence > {relation}.sequence"
            );
            guard
                .execute(
                    &sql,
                    rusqlite::params![group, stream, partition, partition_count, sequence],
                )
                .map_err(|e| Error::Store(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
    }
}
