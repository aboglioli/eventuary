//! SQLite [`MultiplexerStore`] implementation.
//!
//! Records `(event_id, subscriber_id)` completion rows in the
//! configured relation. Event ids are stored as TEXT (UUID string)
//! since SQLite has no native UUID type. `mark_completed` uses
//! `INSERT ... ON CONFLICT DO NOTHING` so concurrent or redelivered
//! calls converge. All SQLite work runs in `spawn_blocking`.

use std::sync::Arc;

use eventuary_core::io::handler::{MultiplexerKey, MultiplexerStore};
use eventuary_core::{Error, Result};

use crate::database::SqliteConn;
use crate::relation::SqliteRelationName;
use crate::schema::{Migration, RelationReplacement};

const MULTIPLEXER_STORE_0001_INIT_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS {multiplexer_completions} (
    event_id      TEXT NOT NULL,
    subscriber_id TEXT NOT NULL,
    completed_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (event_id, subscriber_id)
);
"#;

const MULTIPLEXER_STORE_MIGRATIONS: &[Migration] = &[Migration {
    name: "0001_init",
    sql: MULTIPLEXER_STORE_0001_INIT_SQL,
}];

#[derive(Debug, Clone)]
pub struct SqliteMultiplexerStoreConfig {
    pub relation: SqliteRelationName,
}

impl Default for SqliteMultiplexerStoreConfig {
    fn default() -> Self {
        Self {
            relation: SqliteRelationName::new("multiplexer_completions")
                .expect("default multiplexer relation"),
        }
    }
}

#[derive(Clone)]
pub struct SqliteMultiplexerStore {
    conn: SqliteConn,
    relation: Arc<String>,
}

impl SqliteMultiplexerStore {
    pub fn new(conn: SqliteConn, config: SqliteMultiplexerStoreConfig) -> Self {
        Self {
            conn,
            relation: Arc::new(config.relation.render()),
        }
    }

    pub fn connect(conn: SqliteConn, config: SqliteMultiplexerStoreConfig) -> Result<Self> {
        Self::prepare_schema(&conn, &config)?;
        Ok(Self::new(conn, config))
    }

    pub fn prepare_schema(conn: &SqliteConn, config: &SqliteMultiplexerStoreConfig) -> Result<()> {
        let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
        crate::schema::apply_schema(
            &guard,
            MULTIPLEXER_STORE_MIGRATIONS,
            &[RelationReplacement {
                token: "{multiplexer_completions}",
                relation: &config.relation,
            }],
        )
    }

    pub fn schema_sql(config: &SqliteMultiplexerStoreConfig) -> String {
        crate::schema::render_schema_sql(
            MULTIPLEXER_STORE_MIGRATIONS,
            &[RelationReplacement {
                token: "{multiplexer_completions}",
                relation: &config.relation,
            }],
        )
    }
}

impl MultiplexerStore for SqliteMultiplexerStore {
    async fn is_completed(&self, key: &MultiplexerKey) -> Result<bool> {
        let conn = Arc::clone(&self.conn);
        let relation = Arc::clone(&self.relation);
        let event_id = key.event_id.to_string();
        let subscriber_id = key.subscriber_id.as_str().to_owned();
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            let sql = format!(
                "SELECT 1 FROM {relation} \
                 WHERE event_id = ?1 AND subscriber_id = ?2"
            );
            let row = guard
                .query_row(&sql, rusqlite::params![event_id, subscriber_id], |_| Ok(()))
                .map(|_| true)
                .or_else(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => Ok(false),
                    other => Err(other),
                })
                .map_err(|e| Error::Store(e.to_string()))?;
            Ok(row)
        })
        .await
        .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
    }

    async fn mark_completed(&self, key: &MultiplexerKey) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        let relation = Arc::clone(&self.relation);
        let event_id = key.event_id.to_string();
        let subscriber_id = key.subscriber_id.as_str().to_owned();
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            let sql = format!(
                "INSERT INTO {relation} (event_id, subscriber_id) \
                 VALUES (?1, ?2) \
                 ON CONFLICT (event_id, subscriber_id) DO NOTHING"
            );
            guard
                .execute(&sql, rusqlite::params![event_id, subscriber_id])
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
        let sql = SqliteMultiplexerStore::schema_sql(&SqliteMultiplexerStoreConfig::default());
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"multiplexer_completions\""));
    }
}
