use std::path::Path;
use std::sync::{Arc, Mutex};

use rusqlite::Connection;

use eventuary::{Error, Result};

pub type SqliteConn = Arc<Mutex<Connection>>;

const SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    organization TEXT NOT NULL,
    namespace TEXT NOT NULL,
    topic TEXT NOT NULL,
    event_key TEXT NOT NULL,
    payload TEXT NOT NULL,
    content_type TEXT NOT NULL,
    metadata TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    version INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_org_seq ON events (organization, sequence);
CREATE INDEX IF NOT EXISTS idx_events_org_ns_seq ON events (organization, namespace, sequence);
CREATE INDEX IF NOT EXISTS idx_events_org_topic_seq ON events (organization, topic, sequence);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events (timestamp);

CREATE TABLE IF NOT EXISTS consumer_offsets (
    organization TEXT NOT NULL,
    consumer_group_id TEXT NOT NULL,
    stream TEXT NOT NULL DEFAULT 'default',
    sequence INTEGER NOT NULL,
    PRIMARY KEY (organization, consumer_group_id, stream)
);
"#;

pub struct SqliteDatabase {
    conn: SqliteConn,
}

impl SqliteDatabase {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path).map_err(|e| Error::Store(e.to_string()))?;
        Self::init(conn)
    }

    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory().map_err(|e| Error::Store(e.to_string()))?;
        Self::init(conn)
    }

    fn init(conn: Connection) -> Result<Self> {
        let _: String = conn
            .pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get(0))
            .map_err(|e| Error::Store(e.to_string()))?;
        conn.execute_batch(SCHEMA_SQL)
            .map_err(|e| Error::Store(e.to_string()))?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn conn(&self) -> SqliteConn {
        Arc::clone(&self.conn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_in_memory_creates_schema() {
        let db = SqliteDatabase::open_in_memory().unwrap();
        let conn = db.conn();
        let guard = conn.lock().unwrap();
        let count: i64 = guard
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN ('events', 'consumer_offsets')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn schema_idempotent() {
        let db = SqliteDatabase::open_in_memory().unwrap();
        let conn = db.conn();
        let guard = conn.lock().unwrap();
        guard.execute_batch(SCHEMA_SQL).unwrap();
    }
}
