use std::path::Path;
use std::sync::{Arc, Mutex};

use rusqlite::Connection;

use eventuary_core::{Error, Result};

pub type SqliteConn = Arc<Mutex<Connection>>;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SqliteDatabaseConfig;

pub struct SqliteDatabase {
    conn: SqliteConn,
    config: SqliteDatabaseConfig,
}

impl SqliteDatabase {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path).map_err(|e| Error::Store(e.to_string()))?;
        Self::init(conn, SqliteDatabaseConfig)
    }

    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory().map_err(|e| Error::Store(e.to_string()))?;
        Self::init(conn, SqliteDatabaseConfig)
    }

    pub fn open_with_config(path: impl AsRef<Path>, config: SqliteDatabaseConfig) -> Result<Self> {
        let conn = Connection::open(path).map_err(|e| Error::Store(e.to_string()))?;
        Self::init(conn, config)
    }

    pub fn open_in_memory_with_config(config: SqliteDatabaseConfig) -> Result<Self> {
        let conn = Connection::open_in_memory().map_err(|e| Error::Store(e.to_string()))?;
        Self::init(conn, config)
    }

    fn init(conn: Connection, config: SqliteDatabaseConfig) -> Result<Self> {
        let _: String = conn
            .pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get(0))
            .map_err(|e| Error::Store(e.to_string()))?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            config,
        })
    }

    pub fn conn(&self) -> SqliteConn {
        Arc::clone(&self.conn)
    }

    pub fn config(&self) -> &SqliteDatabaseConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_empty_database_config() {
        let default_config: SqliteDatabaseConfig = Default::default();
        assert_eq!(default_config, SqliteDatabaseConfig);
    }

    #[test]
    fn open_in_memory_creates_no_component_tables() {
        let db = SqliteDatabase::open_in_memory().unwrap();
        let conn = db.conn();
        let guard = conn.lock().unwrap();
        let count: i64 = guard
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }
}
