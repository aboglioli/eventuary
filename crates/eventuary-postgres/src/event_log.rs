use sqlx::PgPool;

use eventuary_core::Result;

use crate::relation::PgRelationName;
use crate::schema::{Migration, RelationReplacement};

const EVENT_LOG_0001_INIT_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS {events} (
    sequence BIGSERIAL PRIMARY KEY,
    id UUID NOT NULL UNIQUE,
    organization TEXT NOT NULL,
    namespace TEXT NOT NULL,
    topic TEXT NOT NULL,
    event_key TEXT NOT NULL,
    payload JSONB NOT NULL,
    content_type TEXT NOT NULL,
    metadata JSONB NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL,
    parent_id UUID,
    correlation_id TEXT,
    causation_id TEXT,
    partition_key TEXT,
    partition_hash BIGINT,
    partition_id BIGINT,
    partition_count BIGINT,
    partition_strategy TEXT
);

CREATE INDEX IF NOT EXISTS idx_events_org_sequence ON {events} (organization, sequence);
CREATE INDEX IF NOT EXISTS idx_events_org_topic_sequence ON {events} (organization, topic, sequence);
CREATE INDEX IF NOT EXISTS idx_events_org_namespace_sequence ON {events} (organization, namespace, sequence);
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
pub(crate) struct PgEventLogSchemaConfig {
    pub(crate) events_relation: PgRelationName,
}

impl Default for PgEventLogSchemaConfig {
    fn default() -> Self {
        Self {
            events_relation: PgRelationName::new("events").expect("default events relation"),
        }
    }
}

pub(crate) struct PgEventLogSchema;

impl PgEventLogSchema {
    pub(crate) fn schema_sql(config: &PgEventLogSchemaConfig) -> String {
        crate::schema::render_schema_sql(EVENT_LOG_MIGRATIONS, &replacements(config))
    }

    pub(crate) async fn prepare(pool: &PgPool, config: &PgEventLogSchemaConfig) -> Result<()> {
        crate::schema::apply_schema(pool, EVENT_LOG_MIGRATIONS, &replacements(config)).await
    }
}

fn replacements(config: &PgEventLogSchemaConfig) -> [RelationReplacement<'_>; 1] {
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
        let sql = PgEventLogSchema::schema_sql(&PgEventLogSchemaConfig::default());
        let normalized = sql
            .replace(',', " ")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        let id = normalized.find("id UUID NOT NULL UNIQUE").unwrap();
        let organization = normalized.find("organization TEXT NOT NULL").unwrap();
        let namespace = normalized.find("namespace TEXT NOT NULL").unwrap();
        let topic = normalized.find("topic TEXT NOT NULL").unwrap();
        let key = normalized.find("event_key TEXT NOT NULL").unwrap();
        let payload = normalized.find("payload JSONB NOT NULL").unwrap();
        assert!(id < organization);
        assert!(organization < namespace);
        assert!(namespace < topic);
        assert!(topic < key);
        assert!(key < payload);
    }

    #[test]
    fn schema_sql_contains_events_table() {
        let sql = PgEventLogSchema::schema_sql(&PgEventLogSchemaConfig::default());
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"events\""));
    }

    #[test]
    fn schema_sql_creates_configured_schema() {
        let config = PgEventLogSchemaConfig {
            events_relation: PgRelationName::new("eventuary.events").unwrap(),
        };
        let sql = PgEventLogSchema::schema_sql(&config);
        assert!(sql.contains("CREATE SCHEMA IF NOT EXISTS \"eventuary\""));
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"eventuary\".\"events\""));
    }
}
