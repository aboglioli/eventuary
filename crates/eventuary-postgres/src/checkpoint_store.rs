use std::sync::Arc;

use serde::{Serialize, de::DeserializeOwned};
use sqlx::{PgPool, Row};

use eventuary_core::io::reader::{CheckpointKey, CheckpointScope, CheckpointStore};
use eventuary_core::io::{Cursor, CursorId};
use eventuary_core::{Error, Result};

use crate::relation::PgRelationName;
use crate::schema::{Migration, RelationReplacement};

const CHECKPOINT_STORE_0001_INIT_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS {offsets} (
    consumer_group_id TEXT  NOT NULL,
    stream_id         TEXT  NOT NULL DEFAULT 'default',
    cursor_id         TEXT  NOT NULL,
    cursor            JSONB NOT NULL,
    cursor_order      BYTEA NOT NULL DEFAULT ''::bytea,
    PRIMARY KEY (consumer_group_id, stream_id, cursor_id)
);
"#;

const CHECKPOINT_STORE_MIGRATIONS: &[Migration] = &[Migration {
    name: "0001_init",
    sql: CHECKPOINT_STORE_0001_INIT_SQL,
}];

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

    pub async fn connect(pool: PgPool, config: PgCheckpointStoreConfig) -> Result<Self> {
        Self::prepare_schema(&pool, &config).await?;
        Ok(Self::new(pool, config))
    }

    pub async fn prepare_schema(pool: &PgPool, config: &PgCheckpointStoreConfig) -> Result<()> {
        crate::schema::apply_schema(
            pool,
            CHECKPOINT_STORE_MIGRATIONS,
            &[RelationReplacement {
                token: "{offsets}",
                relation: &config.offsets_relation,
            }],
        )
        .await
    }

    pub fn schema_sql(config: &PgCheckpointStoreConfig) -> String {
        crate::schema::render_schema_sql(
            CHECKPOINT_STORE_MIGRATIONS,
            &[RelationReplacement {
                token: "{offsets}",
                relation: &config.offsets_relation,
            }],
        )
    }
}

fn encode_cursor_id(cursor_id: &CursorId) -> &str {
    cursor_id.as_str()
}

fn decode_cursor_id(value: String) -> CursorId {
    CursorId::new(value).unwrap_or_else(|_| CursorId::global())
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
    C: Cursor + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    async fn load(&self, key: &CheckpointKey) -> Result<Option<C>> {
        let cursor_id = encode_cursor_id(&key.cursor_id);
        let sql = format!(
            "SELECT cursor FROM {relation} \
             WHERE consumer_group_id = $1 AND stream_id = $2 AND cursor_id = $3",
            relation = self.relation
        );
        let row = sqlx::query(&sql)
            .bind(key.scope.consumer_group_id.as_str())
            .bind(key.scope.stream_id.as_str())
            .bind(cursor_id)
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

    async fn load_scope(&self, scope: &CheckpointScope) -> Result<Vec<(CursorId, C)>> {
        let sql = format!(
            "SELECT cursor_id, cursor FROM {relation} \
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
            let cursor_id: String = row.get("cursor_id");
            let cursor: serde_json::Value = row.get("cursor");
            out.push((decode_cursor_id(cursor_id), decode_cursor::<C>(cursor)?));
        }
        Ok(out)
    }

    async fn commit(&self, key: &CheckpointKey, cursor: C) -> Result<()> {
        let cursor_id = encode_cursor_id(&key.cursor_id);
        let cursor_json = encode_cursor(&cursor)?;
        let cursor_order = cursor.order_key();
        let sql = format!(
            "INSERT INTO {relation} \
               (consumer_group_id, stream_id, cursor_id, cursor, cursor_order) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (consumer_group_id, stream_id, cursor_id) DO UPDATE \
             SET cursor = EXCLUDED.cursor, \
                 cursor_order = EXCLUDED.cursor_order \
             WHERE {relation}.cursor_order < EXCLUDED.cursor_order",
            relation = self.relation
        );
        sqlx::query(&sql)
            .bind(key.scope.consumer_group_id.as_str())
            .bind(key.scope.stream_id.as_str())
            .bind(cursor_id)
            .bind(cursor_json)
            .bind(cursor_order.as_bytes())
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventuary_core::Partition;
    use std::num::NonZeroU16;

    #[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
    struct WrappedCursor {
        sequence: i64,
        partition: Partition,
    }

    #[test]
    fn encode_cursor_preserves_nested_json() {
        let partition = Partition::new(2, NonZeroU16::new(4).unwrap()).unwrap();
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
        assert_eq!(encode_cursor_id(&CursorId::global()), "global");
        assert_eq!(decode_cursor_id("global".to_owned()), CursorId::global());
    }

    #[test]
    fn cursor_id_named_roundtrips_unquoted() {
        let id = CursorId::partition(100, 17);
        let encoded = encode_cursor_id(&id).to_owned();
        assert_eq!(encoded, "partition:100:17");
        assert_eq!(decode_cursor_id(encoded), id);
    }

    #[test]
    fn schema_sql_contains_expected_table() {
        let sql = PgCheckpointStore::<crate::reader::PgCursor>::schema_sql(
            &PgCheckpointStoreConfig::default(),
        );
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"consumer_offsets\""));
    }
}
