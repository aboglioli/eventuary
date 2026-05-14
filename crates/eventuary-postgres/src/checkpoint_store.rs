use std::num::NonZeroU16;
use std::sync::Arc;

use serde::{Serialize, de::DeserializeOwned};
use sqlx::{PgPool, Row};

use eventuary_core::io::checkpoint::{CheckpointKey, CheckpointScope, CheckpointStore};
use eventuary_core::{Error, LogicalPartition, Result};

use crate::relation::PgRelationName;

#[derive(Debug, Clone)]
pub struct PgCheckpointStoreConfig {
    pub offsets_relation: PgRelationName,
}

impl Default for PgCheckpointStoreConfig {
    fn default() -> Self {
        Self {
            offsets_relation: PgRelationName::new("consumer_offsets")
                .expect("default offsets relation"),
        }
    }
}

pub struct PgCheckpointStore<C> {
    pool: PgPool,
    relation: Arc<String>,
    _cursor: std::marker::PhantomData<fn() -> C>,
}

impl<C> Clone for PgCheckpointStore<C> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            relation: Arc::clone(&self.relation),
            _cursor: std::marker::PhantomData,
        }
    }
}

impl<C> PgCheckpointStore<C> {
    pub fn new(pool: PgPool, config: PgCheckpointStoreConfig) -> Self {
        Self {
            pool,
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

fn encode_cursor<C: Serialize>(cursor: &C) -> Result<serde_json::Value> {
    serde_json::to_value(cursor)
        .map_err(|e| Error::Serialization(format!("checkpoint encode: {e}")))
}

fn decode_cursor<C: DeserializeOwned>(value: serde_json::Value) -> Result<C> {
    serde_json::from_value(value)
        .map_err(|e| Error::Serialization(format!("checkpoint decode: {e}")))
}

impl<C> CheckpointStore<C> for PgCheckpointStore<C>
where
    C: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    async fn load(&self, key: &CheckpointKey) -> Result<Option<C>> {
        let (partition, partition_count) = encode_partition(key.partition);
        let sql = format!(
            "SELECT cursor FROM {relation} \
             WHERE consumer_group_id = $1 \
               AND stream_id   = $2 \
               AND partition         = $3 \
               AND partition_count   = $4",
            relation = self.relation
        );
        let row = sqlx::query(&sql)
            .bind(key.scope.consumer_group_id.as_str())
            .bind(key.scope.stream_id.as_str())
            .bind(partition)
            .bind(partition_count)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        match row {
            Some(r) => Ok(Some(decode_cursor::<C>(
                r.get::<serde_json::Value, _>("cursor"),
            )?)),
            None => Ok(None),
        }
    }

    async fn load_scope(
        &self,
        scope: &CheckpointScope,
    ) -> Result<Vec<(Option<LogicalPartition>, C)>> {
        let sql = format!(
            "SELECT partition, partition_count, cursor FROM {relation} \
             WHERE consumer_group_id = $1 AND stream_id = $2",
            relation = self.relation
        );
        let rows = sqlx::query(&sql)
            .bind(scope.consumer_group_id.as_str())
            .bind(scope.stream_id.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let p: i32 = row.get("partition");
            let pc: i32 = row.get("partition_count");
            let cursor: serde_json::Value = row.get("cursor");
            out.push((decode_partition(p, pc), decode_cursor::<C>(cursor)?));
        }
        Ok(out)
    }

    async fn commit(&self, key: &CheckpointKey, cursor: C) -> Result<()> {
        let (partition, partition_count) = encode_partition(key.partition);
        let cursor_json = encode_cursor(&cursor)?;
        let sql = format!(
            "INSERT INTO {relation} \
               (consumer_group_id, stream_id, partition, partition_count, cursor) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (consumer_group_id, stream_id, partition, partition_count) \
             DO UPDATE SET cursor = EXCLUDED.cursor",
            relation = self.relation
        );
        sqlx::query(&sql)
            .bind(key.scope.consumer_group_id.as_str())
            .bind(key.scope.stream_id.as_str())
            .bind(partition)
            .bind(partition_count)
            .bind(cursor_json)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        Ok(())
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
}
