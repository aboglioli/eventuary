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
