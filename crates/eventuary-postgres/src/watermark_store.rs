//! PostgreSQL [`WatermarkStore`] implementation.
//!
//! Persists per-key high-water timestamps. `save_watermark` upserts so
//! redelivered or out-of-order saves converge.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};

use eventuary_core::io::reader::WatermarkStore;
use eventuary_core::{Error, Result};

use crate::relation::PgRelationName;

#[derive(Debug, Clone)]
pub struct PgWatermarkStoreConfig {
    pub relation: PgRelationName,
}

impl Default for PgWatermarkStoreConfig {
    fn default() -> Self {
        Self {
            relation: PgRelationName::new("watermarks").expect("default watermarks relation"),
        }
    }
}

#[derive(Clone)]
pub struct PgWatermarkStore {
    pool: PgPool,
    relation: Arc<String>,
}

impl PgWatermarkStore {
    pub fn new(pool: PgPool, config: PgWatermarkStoreConfig) -> Self {
        Self {
            pool,
            relation: Arc::new(config.relation.render()),
        }
    }
}

impl WatermarkStore for PgWatermarkStore {
    async fn load_watermark(&self, key: &str) -> Result<Option<DateTime<Utc>>> {
        let sql = format!(
            "SELECT to_char(ts AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') AS ts \
             FROM {relation} WHERE key = $1",
            relation = self.relation
        );
        let row = sqlx::query(&sql)
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        match row {
            Some(r) => {
                let ts_str: String = r.get("ts");
                let ts = DateTime::parse_from_rfc3339(&ts_str)
                    .map_err(|e| Error::Serialization(format!("watermark decode: {e}")))?
                    .with_timezone(&Utc);
                Ok(Some(ts))
            }
            None => Ok(None),
        }
    }

    async fn save_watermark(&self, key: &str, ts: DateTime<Utc>) -> Result<()> {
        let sql = format!(
            "INSERT INTO {relation} (key, ts) VALUES ($1, $2::timestamptz) \
             ON CONFLICT (key) DO UPDATE SET ts = EXCLUDED.ts, updated_at = NOW()",
            relation = self.relation
        );
        sqlx::query(&sql)
            .bind(key)
            .bind(ts.to_rfc3339())
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        Ok(())
    }
}
