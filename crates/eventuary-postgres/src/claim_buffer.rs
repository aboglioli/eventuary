//! Postgres-backed `ClaimedBufferStore` implementation.
//!
//! Atomic claim semantics via `FOR UPDATE SKIP LOCKED`: concurrent claim
//! batches receive disjoint sets without explicit synchronization.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use sqlx::{PgPool, Row};

use eventuary_core::io::OwnerId;
use eventuary_core::io::reader::claim_buffer::{ClaimedBufferEntry, ClaimedBufferStore};
use eventuary_core::{Error, Event, Result, SerializedEvent};

use crate::relation::PgRelationName;
use crate::schema::{Migration, RelationReplacement};

const CLAIMED_BUFFER_STORE_0001_INIT_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS {buffer_claims} (
    id              BIGSERIAL    PRIMARY KEY,
    event           JSONB        NOT NULL,
    claimed_by      TEXT         NULL,
    claimed_until   TIMESTAMPTZ  NULL,
    attempts        INT          NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_buffer_claims_pending
ON {buffer_claims} (claimed_until, id)
WHERE claimed_by IS NULL OR claimed_until IS NULL;

CREATE INDEX IF NOT EXISTS idx_buffer_claims_visibility
ON {buffer_claims} (claimed_until)
WHERE claimed_by IS NOT NULL;
"#;

const CLAIMED_BUFFER_STORE_MIGRATIONS: &[Migration] = &[Migration {
    name: "0001_init",
    sql: CLAIMED_BUFFER_STORE_0001_INIT_SQL,
}];

#[derive(Debug, Clone)]
pub struct PgClaimedBufferStoreConfig {
    pub relation: PgRelationName,
}

impl Default for PgClaimedBufferStoreConfig {
    fn default() -> Self {
        Self {
            relation: PgRelationName::new("event_buffer_claims")
                .expect("default buffer claims relation"),
        }
    }
}

pub struct PgClaimedBufferStore {
    pool: PgPool,
    relation: Arc<String>,
}

impl Clone for PgClaimedBufferStore {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            relation: Arc::clone(&self.relation),
        }
    }
}

impl PgClaimedBufferStore {
    pub fn new(pool: PgPool, config: PgClaimedBufferStoreConfig) -> Self {
        Self {
            pool,
            relation: Arc::new(config.relation.render()),
        }
    }

    pub async fn connect(pool: PgPool, config: PgClaimedBufferStoreConfig) -> Result<Self> {
        Self::prepare_schema(&pool, &config).await?;
        Ok(Self::new(pool, config))
    }

    pub async fn prepare_schema(pool: &PgPool, config: &PgClaimedBufferStoreConfig) -> Result<()> {
        crate::schema::apply_schema(
            pool,
            CLAIMED_BUFFER_STORE_MIGRATIONS,
            &[RelationReplacement {
                token: "{buffer_claims}",
                relation: &config.relation,
            }],
        )
        .await
    }

    pub fn schema_sql(config: &PgClaimedBufferStoreConfig) -> String {
        crate::schema::render_schema_sql(
            CLAIMED_BUFFER_STORE_MIGRATIONS,
            &[RelationReplacement {
                token: "{buffer_claims}",
                relation: &config.relation,
            }],
        )
    }
}

fn encode_event(event: &Event) -> Result<serde_json::Value> {
    let serialized = SerializedEvent::from_event(event)?;
    serde_json::to_value(serialized)
        .map_err(|e| Error::Serialization(format!("claim buffer encode event: {e}")))
}

fn decode_event(value: serde_json::Value) -> Result<Event> {
    let serialized: SerializedEvent = serde_json::from_value(value)
        .map_err(|e| Error::Serialization(format!("claim buffer decode event: {e}")))?;
    serialized.to_event()
}

impl ClaimedBufferStore for PgClaimedBufferStore {
    type Id = i64;

    async fn push(&self, event: &Event) -> Result<Self::Id> {
        let sql = format!(
            "INSERT INTO {relation} (event) VALUES ($1) RETURNING id",
            relation = self.relation
        );
        let row = sqlx::query(&sql)
            .bind(encode_event(event)?)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        Ok(row.get::<i64, _>("id"))
    }

    async fn claim_batch(
        &self,
        owner_id: &OwnerId,
        max: usize,
        visibility: Duration,
    ) -> Result<Vec<ClaimedBufferEntry<Self::Id>>> {
        let claimed_until = (Utc::now()
            + chrono::Duration::from_std(visibility)
                .map_err(|_| Error::Config("visibility duration out of range".to_owned()))?)
        .format("%Y-%m-%dT%H:%M:%S%.6fZ")
        .to_string();
        let max_i64 = max as i64;

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| Error::Store(e.to_string()))?;

        let sql = format!(
            "WITH picked AS ( \
                SELECT id FROM {relation} \
                WHERE claimed_by IS NULL OR claimed_until IS NULL OR claimed_until < NOW() \
                ORDER BY id \
                LIMIT $1 \
                FOR UPDATE SKIP LOCKED \
            ) \
            UPDATE {relation} c \
            SET claimed_by = $2, \
                claimed_until = $3::timestamptz, \
                attempts = attempts + 1 \
            FROM picked \
            WHERE c.id = picked.id \
            RETURNING c.id, c.event, c.attempts",
            relation = self.relation
        );

        let rows = sqlx::query(&sql)
            .bind(max_i64)
            .bind(owner_id.as_str())
            .bind(claimed_until)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;

        tx.commit().await.map_err(|e| Error::Store(e.to_string()))?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let id: i64 = row.get("id");
            let event_value: serde_json::Value = row.get("event");
            let attempts: i32 = row.get("attempts");
            out.push(ClaimedBufferEntry {
                id,
                event: decode_event(event_value)?,
                attempts: attempts as u32,
            });
        }

        out.sort_by_key(|e| e.id);

        Ok(out)
    }

    async fn ack(&self, id: &Self::Id) -> Result<()> {
        let sql = format!(
            "DELETE FROM {relation} WHERE id = $1",
            relation = self.relation
        );
        sqlx::query(&sql)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        Ok(())
    }

    async fn nack(&self, id: &Self::Id) -> Result<()> {
        let sql = format!(
            "UPDATE {relation} SET claimed_by = NULL, claimed_until = NULL WHERE id = $1",
            relation = self.relation
        );
        sqlx::query(&sql)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod schema_tests {
    use super::*;

    #[test]
    fn schema_sql_contains_expected_table() {
        let sql = PgClaimedBufferStore::schema_sql(&PgClaimedBufferStoreConfig::default());
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"event_buffer_claims\""));
    }
}
