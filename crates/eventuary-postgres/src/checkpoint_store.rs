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

const SENTINEL_GROUP: &str = "__checkpoint__";

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
        "PgCheckpointStore: cursor must encode as integer (typed sequence)".to_owned(),
    ))
}

fn decode_cursor<C: DeserializeOwned>(sequence: i64) -> Result<C> {
    let value = serde_json::Value::from(sequence);
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
            "SELECT sequence FROM {relation} \
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
            Some(r) => Ok(Some(decode_cursor::<C>(r.get::<i64, _>("sequence"))?)),
            None => Ok(None),
        }
    }

    async fn load_scope(
        &self,
        scope: &CheckpointScope,
    ) -> Result<Vec<(Option<LogicalPartition>, C)>> {
        let sql = format!(
            "SELECT partition, partition_count, sequence FROM {relation} \
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
            let seq: i64 = row.get("sequence");
            out.push((decode_partition(p, pc), decode_cursor::<C>(seq)?));
        }
        Ok(out)
    }

    async fn commit(&self, key: &CheckpointKey, cursor: C) -> Result<()> {
        let (partition, partition_count) = encode_partition(key.partition);
        let sequence = encode_cursor(&cursor)?;
        let sql = format!(
            "INSERT INTO {relation} \
               (consumer_group_id, stream_id, partition, partition_count, sequence) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (consumer_group_id, stream_id, partition, partition_count) \
             DO UPDATE SET sequence = EXCLUDED.sequence \
             WHERE EXCLUDED.sequence > {relation}.sequence",
            relation = self.relation
        );
        sqlx::query(&sql)
            .bind(key.scope.consumer_group_id.as_str())
            .bind(key.scope.stream_id.as_str())
            .bind(partition)
            .bind(partition_count)
            .bind(sequence)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        Ok(())
    }
}

const _: &str = SENTINEL_GROUP;
