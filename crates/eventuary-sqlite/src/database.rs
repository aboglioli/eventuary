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

const MIGRATION_TEMPLATES: &[Migration] = &[
    Migration {
        filename: "0001_init.sql",
        template: include_str!("../migrations/0001_init.sql"),
    },
    Migration {
        filename: "0002_stores.sql",
        template: include_str!("../migrations/0002_stores.sql"),
    },
    Migration {
        filename: "0003_monotonic_checkpoints.sql",
        template: include_str!("../migrations/0003_monotonic_checkpoints.sql"),
    },
    Migration {
        filename: "0004_partition_columns.sql",
        template: include_str!("../migrations/0004_partition_columns.sql"),
    },
    Migration {
        filename: "0005_partition_coordination.sql",
        template: include_str!("../migrations/0005_partition_coordination.sql"),
    },
    Migration {
        filename: "0006_partition_count_coordination.sql",
        template: include_str!("../migrations/0006_partition_count_coordination.sql"),
    },
    Migration {
        filename: "0008_required_event_key.sql",
        template: include_str!("../migrations/0008_required_event_key.sql"),
    },
];

pub fn migrations() -> &'static [Migration] {
    MIGRATION_TEMPLATES
}

#[derive(Debug, Clone)]
pub struct SqliteDatabaseConfig {
    pub events_relation: SqliteRelationName,
    pub offsets_relation: SqliteRelationName,
    pub multiplexer_completions_relation: SqliteRelationName,
    pub dedupe_keys_relation: SqliteRelationName,
    pub buffer_entries_relation: SqliteRelationName,
    pub watermarks_relation: SqliteRelationName,
    pub consumers_relation: SqliteRelationName,
    pub partitions_relation: SqliteRelationName,
}

impl Default for SqliteDatabaseConfig {
    fn default() -> Self {
        Self {
            events_relation: SqliteRelationName::new("events").expect("default events relation"),
            offsets_relation: SqliteRelationName::new("consumer_offsets")
                .expect("default offsets relation"),
            multiplexer_completions_relation: SqliteRelationName::new("multiplexer_completions")
                .expect("default multiplexer relation"),
            dedupe_keys_relation: SqliteRelationName::new("dedupe_keys")
                .expect("default dedupe relation"),
            buffer_entries_relation: SqliteRelationName::new("buffer_entries")
                .expect("default buffer relation"),
            watermarks_relation: SqliteRelationName::new("watermarks")
                .expect("default watermarks relation"),
            consumers_relation: SqliteRelationName::new("event_stream_consumers")
                .expect("default consumers relation"),
            partitions_relation: SqliteRelationName::new("event_stream_partitions")
                .expect("default partitions relation"),
        }
    }
}

pub fn render_migration_sql(migration: &Migration, config: &SqliteDatabaseConfig) -> String {
    migration
        .template
        .replace("{events}", &config.events_relation.render())
        .replace("{offsets}", &config.offsets_relation.render())
        .replace(
            "{multiplexer_completions}",
            &config.multiplexer_completions_relation.render(),
        )
        .replace("{dedupe_keys}", &config.dedupe_keys_relation.render())
        .replace("{buffer_entries}", &config.buffer_entries_relation.render())
        .replace("{watermarks}", &config.watermarks_relation.render())
        .replace("{consumers}", &config.consumers_relation.render())
        .replace("{partitions}", &config.partitions_relation.render())
}

pub fn render_schema_sql(config: &SqliteDatabaseConfig) -> String {
    let mut sql = String::new();
    for migration in migrations() {
        sql.push_str(&render_migration_sql(migration, config));
        if !sql.ends_with('\n') {
            sql.push('\n');
        }
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
    for migration in migrations() {
        if migration.filename == "0008_required_event_key.sql"
            && event_key_already_not_null(conn, config)?
        {
            continue;
        }

        let sql = render_migration_sql(migration, config);
        for statement in sql.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            let result = conn.execute(statement, []);
            match result {
                Ok(_) => {}
                Err(ref e) if is_duplicate_add_column_error(statement, e) => {}
                Err(e) => return Err(Error::Store(e.to_string())),
            }
        }
    }
    Ok(())
}

fn event_key_already_not_null(conn: &Connection, config: &SqliteDatabaseConfig) -> Result<bool> {
    let events_table = config.events_relation.table();
    let pragma_sql = format!("PRAGMA table_info(\"{}\")", events_table);
    let mut stmt = conn
        .prepare(&pragma_sql)
        .map_err(|e| Error::Store(e.to_string()))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| Error::Store(e.to_string()))?;
    while let Some(row) = rows.next().map_err(|e| Error::Store(e.to_string()))? {
        let name: String = row.get(1).map_err(|e| Error::Store(e.to_string()))?;
        if name == "event_key" {
            let notnull: i64 = row.get(3).map_err(|e| Error::Store(e.to_string()))?;
            return Ok(notnull == 1);
        }
    }
    Ok(false)
}

fn is_duplicate_add_column_error(statement: &str, e: &rusqlite::Error) -> bool {
    is_add_column_statement(statement) && e.to_string().contains("duplicate column name")
}

fn is_add_column_statement(statement: &str) -> bool {
    let normalized = statement
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_uppercase();
    normalized.starts_with("ALTER TABLE ") && normalized.contains(" ADD COLUMN ")
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

    use rusqlite::Connection;

    fn sqlite_master_count(conn: &Connection, object_type: &str, name: &str) -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = ?1 AND name = ?2",
            rusqlite::params![object_type, name],
            |row| row.get(0),
        )
        .unwrap()
    }

    #[test]
    fn migrations_continue_after_duplicate_add_column() {
        let conn = Connection::open_in_memory().unwrap();
        let config = SqliteDatabaseConfig::default();

        for migration in &migrations()[..5] {
            let sql = render_migration_sql(migration, &config);
            conn.execute_batch(&sql).unwrap();
        }

        assert_eq!(
            sqlite_master_count(
                &conn,
                "index",
                "idx_event_stream_partitions_group_stream_count"
            ),
            0
        );

        apply_migrations(&conn, &config).unwrap();

        assert_eq!(
            sqlite_master_count(
                &conn,
                "index",
                "idx_event_stream_partitions_group_stream_count"
            ),
            1
        );
    }

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
        let sql = render_migration_sql(&migrations()[0], &SqliteDatabaseConfig::default());
        guard.execute_batch(&sql).unwrap();
    }

    #[test]
    fn schema_sql_is_available_for_manual_migrations() {
        let sql = schema_sql();
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"events\""));
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"consumer_offsets\""));
    }

    #[test]
    fn opens_idempotently_with_alternate_relations_after_default_relations() {
        // First, open with default relations so `events` + `consumer_offsets` exist.
        let db = SqliteDatabase::open_in_memory().unwrap();
        let conn = db.conn();

        // Reuse the same connection with a custom relation pair. The
        // alternate tables must be created, not skipped by a global
        // migration version.
        let alt = SqliteDatabaseConfig {
            events_relation: SqliteRelationName::new("alt_events").unwrap(),
            offsets_relation: SqliteRelationName::new("alt_offsets").unwrap(),
            ..SqliteDatabaseConfig::default()
        };
        let sql = render_migration_sql(&migrations()[0], &alt);
        {
            let guard = conn.lock().unwrap();
            guard.execute_batch(&sql).unwrap();
        }
        let guard = conn.lock().unwrap();
        let alt_count: i64 = guard
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN ('alt_events', 'alt_offsets')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(alt_count, 2, "alternate relations must be created");
    }

    #[test]
    fn schema_sql_uses_configured_relations() {
        let config = SqliteDatabaseConfig {
            events_relation: SqliteRelationName::new("custom_events").unwrap(),
            offsets_relation: SqliteRelationName::new("custom_offsets").unwrap(),
            ..SqliteDatabaseConfig::default()
        };
        let sql = render_schema_sql(&config);
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"custom_events\""));
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"custom_offsets\""));
    }

    #[test]
    fn apply_migrations_can_run_twice() {
        let conn = Connection::open_in_memory().unwrap();
        let config = SqliteDatabaseConfig::default();

        apply_migrations(&conn, &config).unwrap();

        conn.execute(
            "INSERT INTO events (id, organization, namespace, topic, event_key, payload, content_type, metadata, timestamp, version) \
             VALUES ('id-1', 'org', 'ns', 'topic', 'key-1', '{}', 'application/json', '{}', '2024-01-01T00:00:00Z', 1)",
            [],
        )
        .unwrap();

        apply_migrations(&conn, &config).unwrap();

        let row_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(row_count, 1, "event row must survive second apply_migrations");

        let temp_table_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'eventuary_required_key_migration_events'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            temp_table_count, 0,
            "migration temp table must not exist after second apply_migrations (guard skipped rebuild)"
        );

        let is_not_null = {
            let config = SqliteDatabaseConfig::default();
            event_key_already_not_null(&conn, &config).unwrap()
        };
        assert!(is_not_null, "event_key must be NOT NULL after second apply_migrations");
    }
}
