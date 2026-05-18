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
