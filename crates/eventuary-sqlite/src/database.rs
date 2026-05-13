use std::path::Path;
use std::sync::{Arc, Mutex};

use rusqlite::Connection;

use eventuary_core::{Error, Result};

use crate::relation::SqliteRelationName;

pub type SqliteConn = Arc<Mutex<Connection>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Migration {
    pub filename: &'static str,
    pub template: &'static str,
}

impl Migration {
    pub fn version(&self) -> i64 {
        migration_version(self.filename)
    }
}

const MIGRATION_TEMPLATES: &[Migration] = &[Migration {
    filename: "0001_init.sql",
    template: include_str!("../migrations/0001_init.sql"),
}];

pub fn migrations() -> &'static [Migration] {
    MIGRATION_TEMPLATES
}

#[derive(Debug, Clone)]
pub struct SqliteDatabaseConfig {
    pub events_relation: SqliteRelationName,
    pub offsets_relation: SqliteRelationName,
}

impl Default for SqliteDatabaseConfig {
    fn default() -> Self {
        Self {
            events_relation: SqliteRelationName::new("events").expect("default events relation"),
            offsets_relation: SqliteRelationName::new("consumer_offsets")
                .expect("default offsets relation"),
        }
    }
}

pub fn render_migration_sql(migration: &Migration, config: &SqliteDatabaseConfig) -> String {
    migration
        .template
        .replace("{events}", &config.events_relation.render())
        .replace("{offsets}", &config.offsets_relation.render())
}

pub fn render_schema_sql(config: &SqliteDatabaseConfig) -> String {
    let mut sql = String::new();
    for migration in migrations() {
        sql.push_str(&render_migration_sql(migration, config));
        if !sql.ends_with('\n') {
            sql.push('\n');
        }
        sql.push_str(&format!("PRAGMA user_version = {};\n", migration.version()));
    }
    sql
}

pub fn schema_sql() -> String {
    render_schema_sql(&SqliteDatabaseConfig::default())
}

pub struct SqliteDatabase {
    conn: SqliteConn,
    config: SqliteDatabaseConfig,
}

impl SqliteDatabase {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path).map_err(|e| Error::Store(e.to_string()))?;
        Self::init(conn, SqliteDatabaseConfig::default())
    }

    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory().map_err(|e| Error::Store(e.to_string()))?;
        Self::init(conn, SqliteDatabaseConfig::default())
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
        apply_migrations(&conn, &config)?;

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

fn apply_migrations(conn: &Connection, config: &SqliteDatabaseConfig) -> Result<()> {
    let version = current_schema_version(conn)?;
    for migration in migrations() {
        let migration_version = migration.version();
        if migration_version <= version {
            continue;
        }
        let sql = render_migration_sql(migration, config);
        conn.execute_batch(&sql)
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
        let sql = render_migration_sql(&migrations()[0], &SqliteDatabaseConfig::default());
        guard.execute_batch(&sql).unwrap();
    }

    #[test]
    fn schema_sql_is_available_for_manual_migrations() {
        let sql = schema_sql();
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"events\""));
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"consumer_offsets\""));
        assert!(sql.contains("PRAGMA user_version = 1"));
    }

    #[test]
    fn schema_sql_uses_configured_relations() {
        let config = SqliteDatabaseConfig {
            events_relation: SqliteRelationName::new("custom_events").unwrap(),
            offsets_relation: SqliteRelationName::new("custom_offsets").unwrap(),
        };
        let sql = render_schema_sql(&config);
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"custom_events\""));
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"custom_offsets\""));
    }
}
