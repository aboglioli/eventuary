use rusqlite::Connection;

use eventuary_core::{Error, Result};

use crate::relation::SqliteRelationName;
use crate::schema::{Migration, RelationReplacement};

const EVENT_LOG_0001_INIT_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS {events} (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    organization TEXT NOT NULL,
    namespace TEXT NOT NULL,
    topic TEXT NOT NULL,
    event_key TEXT NOT NULL,
    payload TEXT NOT NULL,
    content_type TEXT NOT NULL,
    metadata TEXT NOT NULL,
    partition_key TEXT,
    partition_hash INTEGER,
    partition_id INTEGER,
    partition_count INTEGER,
    partition_strategy TEXT,
    timestamp TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    spec_version INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_org_seq ON {events} (organization, sequence);
CREATE INDEX IF NOT EXISTS idx_events_org_ns_seq ON {events} (organization, namespace, sequence);
CREATE INDEX IF NOT EXISTS idx_events_org_topic_seq ON {events} (organization, topic, sequence);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON {events} (timestamp);

CREATE INDEX IF NOT EXISTS idx_events_partition_count_id_sequence
ON {events} (partition_count, partition_id, sequence)
WHERE partition_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_events_org_ns_partition_count_id_sequence
ON {events} (organization, namespace, partition_count, partition_id, sequence)
WHERE partition_id IS NOT NULL;
"#;

const EVENT_LOG_MIGRATIONS: &[Migration] = &[Migration {
    name: "0001_init",
    sql: EVENT_LOG_0001_INIT_SQL,
}];

#[derive(Debug, Clone)]
pub(crate) struct SqliteEventLogSchemaConfig {
    pub(crate) events_relation: SqliteRelationName,
}

impl Default for SqliteEventLogSchemaConfig {
    fn default() -> Self {
        Self {
            events_relation: SqliteRelationName::new("events").expect("default events relation"),
        }
    }
}

pub(crate) struct SqliteEventLogSchema;

impl SqliteEventLogSchema {
    pub(crate) fn schema_sql(config: &SqliteEventLogSchemaConfig) -> String {
        crate::schema::render_schema_sql(EVENT_LOG_MIGRATIONS, &replacements(config))
    }

    pub(crate) fn prepare(conn: &Connection, config: &SqliteEventLogSchemaConfig) -> Result<()> {
        crate::schema::apply_schema(conn, EVENT_LOG_MIGRATIONS, &replacements(config))
    }
}

pub(crate) fn decode_version(value: i64) -> Result<u32> {
    u32::try_from(value).map_err(|e| Error::Serialization(format!("decode version {value}: {e}")))
}

fn replacements(config: &SqliteEventLogSchemaConfig) -> [RelationReplacement<'_>; 1] {
    [RelationReplacement {
        token: "{events}",
        relation: &config.events_relation,
    }]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_orders_required_key_after_topic() {
        let sql = SqliteEventLogSchema::schema_sql(&SqliteEventLogSchemaConfig::default());
        let normalized = sql
            .replace(',', " ")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        let id = normalized.find("id TEXT NOT NULL UNIQUE").unwrap();
        let organization = normalized.find("organization TEXT NOT NULL").unwrap();
        let namespace = normalized.find("namespace TEXT NOT NULL").unwrap();
        let topic = normalized.find("topic TEXT NOT NULL").unwrap();
        let key = normalized.find("event_key TEXT NOT NULL").unwrap();
        let payload = normalized.find("payload TEXT NOT NULL").unwrap();
        assert!(id < organization);
        assert!(organization < namespace);
        assert!(namespace < topic);
        assert!(topic < key);
        assert!(key < payload);
    }

    #[test]
    fn the_events_table_keeps_partitioning_before_time_and_versions() {
        let conn = Connection::open_in_memory().unwrap();
        SqliteEventLogSchema::prepare(&conn, &SqliteEventLogSchemaConfig::default()).unwrap();

        let mut statement = conn.prepare("PRAGMA table_info(\"events\")").unwrap();
        let columns: Vec<String> = statement
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .map(|name| name.unwrap())
            .collect();

        assert_eq!(
            columns,
            [
                "sequence",
                "id",
                "organization",
                "namespace",
                "topic",
                "event_key",
                "payload",
                "content_type",
                "metadata",
                "partition_key",
                "partition_hash",
                "partition_id",
                "partition_count",
                "partition_strategy",
                "timestamp",
                "schema_version",
                "spec_version",
            ]
        );
    }

    #[test]
    fn schema_sql_contains_events_table() {
        let sql = SqliteEventLogSchema::schema_sql(&SqliteEventLogSchemaConfig::default());
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"events\""));
    }
}
