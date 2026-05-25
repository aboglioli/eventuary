//! PostgreSQL [`MultiplexerStore`] implementation.
//!
//! Records `(event_id, subscriber_id)` completion rows in the
//! configured relation. `mark_completed` uses
//! `INSERT ... ON CONFLICT DO NOTHING` so concurrent or redelivered
//! calls converge without conflict.

use std::sync::Arc;

use sqlx::PgPool;

use eventuary_core::io::handler::{MultiplexerKey, MultiplexerStore};
use eventuary_core::{Error, Result};

use crate::relation::PgRelationName;
use crate::schema::{Migration, RelationReplacement};

const MULTIPLEXER_STORE_0001_INIT_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS {multiplexer_completions} (
    event_id      UUID        NOT NULL,
    subscriber_id TEXT        NOT NULL,
    completed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, subscriber_id)
);
"#;

const MULTIPLEXER_STORE_MIGRATIONS: &[Migration] = &[Migration {
    name: "0001_init",
    sql: MULTIPLEXER_STORE_0001_INIT_SQL,
}];

#[derive(Debug, Clone)]
pub struct PgMultiplexerStoreConfig {
    pub relation: PgRelationName,
}

impl Default for PgMultiplexerStoreConfig {
    fn default() -> Self {
        Self {
            relation: PgRelationName::new("multiplexer_completions")
                .expect("default multiplexer relation"),
        }
    }
}

#[derive(Clone)]
pub struct PgMultiplexerStore {
    pool: PgPool,
    relation: Arc<String>,
}

impl PgMultiplexerStore {
    pub fn new(pool: PgPool, config: PgMultiplexerStoreConfig) -> Self {
        Self {
            pool,
            relation: Arc::new(config.relation.render()),
        }
    }

    pub async fn connect(pool: PgPool, config: PgMultiplexerStoreConfig) -> Result<Self> {
        Self::prepare_schema(&pool, &config).await?;
        Ok(Self::new(pool, config))
    }

    pub async fn prepare_schema(pool: &PgPool, config: &PgMultiplexerStoreConfig) -> Result<()> {
        crate::schema::apply_schema(
            pool,
            MULTIPLEXER_STORE_MIGRATIONS,
            &[RelationReplacement {
                token: "{multiplexer_completions}",
                relation: &config.relation,
            }],
        )
        .await
    }

    pub fn schema_sql(config: &PgMultiplexerStoreConfig) -> String {
        crate::schema::render_schema_sql(
            MULTIPLEXER_STORE_MIGRATIONS,
            &[RelationReplacement {
                token: "{multiplexer_completions}",
                relation: &config.relation,
            }],
        )
    }
}

impl MultiplexerStore for PgMultiplexerStore {
    async fn is_completed(&self, key: &MultiplexerKey) -> Result<bool> {
        let event_id = key.event_id.to_string();
        let sql = format!(
            "SELECT 1 FROM {relation} \
             WHERE event_id = $1::uuid AND subscriber_id = $2",
            relation = self.relation
        );
        let row = sqlx::query(&sql)
            .bind(event_id)
            .bind(key.subscriber_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        Ok(row.is_some())
    }

    async fn mark_completed(&self, key: &MultiplexerKey) -> Result<()> {
        let event_id = key.event_id.to_string();
        let sql = format!(
            "INSERT INTO {relation} (event_id, subscriber_id) \
             VALUES ($1::uuid, $2) \
             ON CONFLICT (event_id, subscriber_id) DO NOTHING",
            relation = self.relation
        );
        sqlx::query(&sql)
            .bind(event_id)
            .bind(key.subscriber_id.as_str())
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
        let sql = PgMultiplexerStore::schema_sql(&PgMultiplexerStoreConfig::default());
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"multiplexer_completions\""));
    }
}
