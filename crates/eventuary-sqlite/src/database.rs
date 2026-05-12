use std::path::Path;
use std::sync::{Arc, Mutex};

use rusqlite::Connection;

use eventuary_core::{Error, Result};

pub type SqliteConn = Arc<Mutex<Connection>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Migration {
    pub filename: &'static str,
    pub sql: &'static str,
}

impl Migration {
    pub fn version(&self) -> i64 {
        migration_version(self.filename)
    }
}

macro_rules! migration {
    ($filename:literal) => {
        Migration {
            filename: $filename,
            sql: include_str!(concat!("../migrations/", $filename)),
        }
    };
}

const MIGRATIONS: &[Migration] = &[migration!("0001_init.sql")];

pub fn migrations() -> &'static [Migration] {
    MIGRATIONS
}

pub fn schema_sql() -> String {
    let mut sql = String::new();
    for migration in migrations() {
        sql.push_str(migration.sql);
        if !sql.ends_with('\n') {
            sql.push('\n');
        }
        sql.push_str(&format!("PRAGMA user_version = {};\n", migration.version()));
    }
    sql
}

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
        apply_migrations(&conn)?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn conn(&self) -> SqliteConn {
        Arc::clone(&self.conn)
    }
}

fn apply_migrations(conn: &Connection) -> Result<()> {
    let version = current_schema_version(conn)?;
    for migration in migrations() {
        let migration_version = migration.version();
        if migration_version <= version {
            continue;
        }
        conn.execute_batch(migration.sql)
            .map_err(|e| Error::Store(e.to_string()))?;
        conn.pragma_update(None, "user_version", migration_version)
            .map_err(|e| Error::Store(e.to_string()))?;
    }
    Ok(())
}

fn current_schema_version(conn: &Connection) -> Result<i64> {
    conn.pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(|e| Error::Store(e.to_string()))
}

fn migration_version(filename: &str) -> i64 {
    let Some((version, _)) = filename.split_once('_') else {
        panic!("migration filename must start with a numeric version prefix")
    };
    version
        .parse()
        .expect("migration filename version prefix must be numeric")
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
        let lineage_columns: i64 = guard
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('events') WHERE name IN ('parent_id', 'correlation_id', 'causation_id')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(lineage_columns, 3);
        let version: i64 = guard
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 1);
    }

    #[test]
    fn schema_idempotent() {
        let db = SqliteDatabase::open_in_memory().unwrap();
        let conn = db.conn();
        let guard = conn.lock().unwrap();
        guard.execute_batch(migrations()[0].sql).unwrap();
    }

    #[test]
    fn schema_sql_is_available_for_manual_migrations() {
        let sql: String = schema_sql();
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS events"));
        assert!(sql.contains("parent_id TEXT"));
        assert!(sql.contains("event_key TEXT"));
        assert!(sql.contains("checkpoint_name"));
        assert!(sql.contains("partition         INTEGER NOT NULL DEFAULT 0"));
        assert!(sql.contains("partition_count   INTEGER NOT NULL DEFAULT 1"));
        assert!(sql.contains(
            "PRIMARY KEY (consumer_group_id, checkpoint_name, partition, partition_count)"
        ));
        assert!(sql.contains("PRAGMA user_version = 1"));
        assert!(sql.contains(migrations()[0].sql.trim()));
        assert_eq!(migrations()[0].filename, "0001_init.sql");
        assert_eq!(migrations()[0].version(), 1);
    }
}
