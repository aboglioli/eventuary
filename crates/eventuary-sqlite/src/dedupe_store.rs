//! SQLite [`DedupeStore`] implementation.
//!
//! Keyed by event id (stored as TEXT). `mark_if_new` overrides the
//! default exists+mark path with a single
//! `INSERT ... ON CONFLICT DO NOTHING RETURNING` so concurrent dedupe
//! checks converge without a race. All SQLite work runs in
//! `spawn_blocking`.

use std::sync::Arc;

use eventuary_core::io::reader::DedupeStore;
use eventuary_core::{Error, Event, Result};

use crate::database::SqliteConn;
use crate::relation::SqliteRelationName;
use crate::schema::{Migration, RelationReplacement};

const DEDUPE_STORE_0001_INIT_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS {dedupe_keys} (
    event_id     TEXT NOT NULL PRIMARY KEY,
    processed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"#;

const DEDUPE_STORE_MIGRATIONS: &[Migration] = &[Migration {
    name: "0001_init",
    sql: DEDUPE_STORE_0001_INIT_SQL,
}];

#[derive(Debug, Clone)]
pub struct SqliteDedupeStoreConfig {
    pub relation: SqliteRelationName,
}

impl Default for SqliteDedupeStoreConfig {
    fn default() -> Self {
        Self {
            relation: SqliteRelationName::new("dedupe_keys").expect("default dedupe relation"),
        }
    }
}

#[derive(Clone)]
pub struct SqliteDedupeStore {
    conn: SqliteConn,
    relation: Arc<String>,
}

impl SqliteDedupeStore {
    pub fn new(conn: SqliteConn, config: SqliteDedupeStoreConfig) -> Self {
        Self {
            conn,
            relation: Arc::new(config.relation.render()),
        }
    }

    pub fn connect(conn: SqliteConn, config: SqliteDedupeStoreConfig) -> Result<Self> {
        Self::prepare_schema(&conn, &config)?;
        Ok(Self::new(conn, config))
    }

    pub fn prepare_schema(conn: &SqliteConn, config: &SqliteDedupeStoreConfig) -> Result<()> {
        let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
        crate::schema::apply_schema(
            &guard,
            DEDUPE_STORE_MIGRATIONS,
            &[RelationReplacement {
                token: "{dedupe_keys}",
                relation: &config.relation,
            }],
        )
    }

    pub fn schema_sql(config: &SqliteDedupeStoreConfig) -> String {
        crate::schema::render_schema_sql(
            DEDUPE_STORE_MIGRATIONS,
            &[RelationReplacement {
                token: "{dedupe_keys}",
                relation: &config.relation,
            }],
        )
    }
}

impl DedupeStore for SqliteDedupeStore {
    async fn exists(&self, event: &Event) -> Result<bool> {
        let conn = Arc::clone(&self.conn);
        let relation = Arc::clone(&self.relation);
        let event_id = event.id().to_string();
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            let sql = format!("SELECT 1 FROM {relation} WHERE event_id = ?1");
            let found = guard
                .query_row(&sql, rusqlite::params![event_id], |_| Ok(()))
                .map(|_| true)
                .or_else(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => Ok(false),
                    other => Err(other),
                })
                .map_err(|e| Error::Store(e.to_string()))?;
            Ok(found)
        })
        .await
        .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
    }

    async fn mark_processed(&self, event: &Event) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        let relation = Arc::clone(&self.relation);
        let event_id = event.id().to_string();
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            let sql = format!(
                "INSERT INTO {relation} (event_id) VALUES (?1) \
                 ON CONFLICT (event_id) DO NOTHING"
            );
            guard
                .execute(&sql, rusqlite::params![event_id])
                .map_err(|e| Error::Store(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
    }

    async fn mark_if_new(&self, event: &Event) -> Result<bool> {
        let conn = Arc::clone(&self.conn);
        let relation = Arc::clone(&self.relation);
        let event_id = event.id().to_string();
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            let sql = format!(
                "INSERT INTO {relation} (event_id) VALUES (?1) \
                 ON CONFLICT (event_id) DO NOTHING \
                 RETURNING event_id"
            );
            let inserted = guard
                .query_row(&sql, rusqlite::params![event_id], |r| r.get::<_, String>(0))
                .map(|_| true)
                .or_else(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => Ok(false),
                    other => Err(other),
                })
                .map_err(|e| Error::Store(e.to_string()))?;
            Ok(inserted)
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
        let sql = SqliteDedupeStore::schema_sql(&SqliteDedupeStoreConfig::default());
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"dedupe_keys\""));
    }
}
