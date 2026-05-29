//! SQLite [`WatermarkStore`] implementation.
//!
//! Persists per-key high-water timestamps. Timestamps round-trip
//! through RFC3339 TEXT. `save_watermark` upserts so redelivered or
//! out-of-order saves converge. All SQLite work runs in
//! `spawn_blocking`.

use std::sync::Arc;

use chrono::{DateTime, Utc};

use eventuary_core::io::reader::WatermarkStore;
use eventuary_core::{Error, Result};

use crate::database::SqliteConn;
use crate::relation::SqliteRelationName;
use crate::schema::{Migration, RelationReplacement};

const WATERMARK_STORE_0001_INIT_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS {watermarks} (
    key        TEXT NOT NULL PRIMARY KEY,
    ts         TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"#;

const WATERMARK_STORE_MIGRATIONS: &[Migration] = &[Migration {
    name: "0001_init",
    sql: WATERMARK_STORE_0001_INIT_SQL,
}];

#[derive(Debug, Clone)]
pub struct SqliteWatermarkStoreConfig {
    pub relation: SqliteRelationName,
}

impl Default for SqliteWatermarkStoreConfig {
    fn default() -> Self {
        Self {
            relation: SqliteRelationName::new("watermarks").expect("default watermarks relation"),
        }
    }
}

#[derive(Clone)]
pub struct SqliteWatermarkStore {
    conn: SqliteConn,
    relation: Arc<String>,
}

impl SqliteWatermarkStore {
    pub fn new(conn: SqliteConn, config: SqliteWatermarkStoreConfig) -> Self {
        Self {
            conn,
            relation: Arc::new(config.relation.render()),
        }
    }

    pub fn connect(conn: SqliteConn, config: SqliteWatermarkStoreConfig) -> Result<Self> {
        Self::prepare_schema(&conn, &config)?;
        Ok(Self::new(conn, config))
    }

    pub fn prepare_schema(conn: &SqliteConn, config: &SqliteWatermarkStoreConfig) -> Result<()> {
        let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
        crate::schema::apply_schema(
            &guard,
            WATERMARK_STORE_MIGRATIONS,
            &[RelationReplacement {
                token: "{watermarks}",
                relation: &config.relation,
            }],
        )
    }

    pub fn schema_sql(config: &SqliteWatermarkStoreConfig) -> String {
        crate::schema::render_schema_sql(
            WATERMARK_STORE_MIGRATIONS,
            &[RelationReplacement {
                token: "{watermarks}",
                relation: &config.relation,
            }],
        )
    }
}

impl WatermarkStore for SqliteWatermarkStore {
    async fn load_watermark(&self, key: &str) -> Result<Option<DateTime<Utc>>> {
        let conn = Arc::clone(&self.conn);
        let relation = Arc::clone(&self.relation);
        let key = key.to_owned();
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            let sql = format!("SELECT ts FROM {relation} WHERE key = ?1");
            let ts_str = guard
                .query_row(&sql, rusqlite::params![key], |r| r.get::<_, String>(0))
                .map(Some)
                .or_else(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => Ok(None),
                    other => Err(other),
                })
                .map_err(|e| Error::Store(e.to_string()))?;
            match ts_str {
                Some(s) => {
                    let ts = DateTime::parse_from_rfc3339(&s)
                        .map_err(|e| Error::Serialization(format!("watermark decode: {e}")))?
                        .with_timezone(&Utc);
                    Ok(Some(ts))
                }
                None => Ok(None),
            }
        })
        .await
        .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
    }

    async fn save_watermark(&self, key: &str, ts: DateTime<Utc>) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        let relation = Arc::clone(&self.relation);
        let key = key.to_owned();
        let ts_str = ts.to_rfc3339();
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            let sql = format!(
                "INSERT INTO {relation} (key, ts) VALUES (?1, ?2) \
                 ON CONFLICT (key) DO UPDATE SET ts = excluded.ts, updated_at = CURRENT_TIMESTAMP"
            );
            guard
                .execute(&sql, rusqlite::params![key, ts_str])
                .map_err(|e| Error::Store(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
    }
}

#[cfg(test)]
mod schema_tests {
    use super::*;

    #[test]
    fn schema_sql_contains_expected_table() {
        let sql = SqliteWatermarkStore::schema_sql(&SqliteWatermarkStoreConfig::default());
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"watermarks\""));
    }
}
