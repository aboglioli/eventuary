//! PostgreSQL [`DedupeStore`] implementation.
//!
//! Keyed by event id (UUID). `mark_if_new` overrides the default
//! exists+mark path with a single `INSERT ... ON CONFLICT DO NOTHING`
//! so concurrent dedupe checks converge without a race.

use std::sync::Arc;

use sqlx::PgPool;

use eventuary_core::io::reader::DedupeStore;
use eventuary_core::{Error, Event, Result};

use crate::relation::PgRelationName;
use crate::schema::{Migration, RelationReplacement};

const DEDUPE_STORE_0001_INIT_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS {dedupe_keys} (
    event_id     UUID        NOT NULL PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"#;

const DEDUPE_STORE_MIGRATIONS: &[Migration] = &[Migration {
    name: "0001_init",
    sql: DEDUPE_STORE_0001_INIT_SQL,
}];

#[derive(Debug, Clone)]
pub struct PgDedupeStoreConfig {
    pub relation: PgRelationName,
}

impl Default for PgDedupeStoreConfig {
    fn default() -> Self {
        Self {
            relation: PgRelationName::new("dedupe_keys").expect("default dedupe relation"),
        }
    }
}

#[derive(Clone)]
pub struct PgDedupeStore {
    pool: PgPool,
    relation: Arc<String>,
}

impl PgDedupeStore {
    pub fn new(pool: PgPool, config: PgDedupeStoreConfig) -> Self {
        Self {
            pool,
            relation: Arc::new(config.relation.render()),
        }
    }

    pub async fn connect(pool: PgPool, config: PgDedupeStoreConfig) -> Result<Self> {
        Self::prepare_schema(&pool, &config).await?;
        Ok(Self::new(pool, config))
    }

    pub async fn prepare_schema(pool: &PgPool, config: &PgDedupeStoreConfig) -> Result<()> {
        crate::schema::apply_schema(
            pool,
            DEDUPE_STORE_MIGRATIONS,
            &[RelationReplacement {
                token: "{dedupe_keys}",
                relation: &config.relation,
            }],
        )
        .await
    }

    pub fn schema_sql(config: &PgDedupeStoreConfig) -> String {
        crate::schema::render_schema_sql(
            DEDUPE_STORE_MIGRATIONS,
            &[RelationReplacement {
                token: "{dedupe_keys}",
                relation: &config.relation,
            }],
        )
    }
}

impl DedupeStore for PgDedupeStore {
    async fn exists(&self, event: &Event) -> Result<bool> {
        let event_id = event.id().to_string();
        let sql = format!(
            "SELECT 1 FROM {relation} WHERE event_id = $1::uuid",
            relation = self.relation
        );
        let row = sqlx::query(&sql)
            .bind(event_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        Ok(row.is_some())
    }

    async fn mark_processed(&self, event: &Event) -> Result<()> {
        let event_id = event.id().to_string();
        let sql = format!(
            "INSERT INTO {relation} (event_id) VALUES ($1::uuid) ON CONFLICT (event_id) DO NOTHING",
            relation = self.relation
        );
        sqlx::query(&sql)
            .bind(event_id)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        Ok(())
    }

    async fn mark_if_new(&self, event: &Event) -> Result<bool> {
        let event_id = event.id().to_string();
        let sql = format!(
            "INSERT INTO {relation} (event_id) VALUES ($1::uuid) \
             ON CONFLICT (event_id) DO NOTHING \
             RETURNING event_id",
            relation = self.relation
        );
        let row = sqlx::query(&sql)
            .bind(event_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        Ok(row.is_some())
    }
}

#[cfg(test)]
mod schema_tests {
    use super::*;

    #[test]
    fn schema_sql_contains_expected_table() {
        let sql = PgDedupeStore::schema_sql(&PgDedupeStoreConfig::default());
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"dedupe_keys\""));
    }
}
