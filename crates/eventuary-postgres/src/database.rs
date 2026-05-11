use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use eventuary_core::{Error, Result};

const SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS events (
    sequence BIGSERIAL PRIMARY KEY,
    id UUID NOT NULL UNIQUE,
    organization TEXT NOT NULL,
    namespace TEXT NOT NULL,
    topic TEXT NOT NULL,
    event_key TEXT,
    payload JSONB NOT NULL,
    content_type TEXT NOT NULL,
    metadata JSONB NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL,
    parent_id UUID,
    correlation_id TEXT,
    causation_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_events_org_sequence ON events (organization, sequence);
CREATE INDEX IF NOT EXISTS idx_events_org_topic_sequence ON events (organization, topic, sequence);
CREATE INDEX IF NOT EXISTS idx_events_org_namespace_sequence ON events (organization, namespace, sequence);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events (timestamp);

CREATE TABLE IF NOT EXISTS consumer_offsets (
    organization TEXT NOT NULL,
    consumer_group_id TEXT NOT NULL,
    stream TEXT NOT NULL DEFAULT 'default',
    sequence BIGINT NOT NULL,
    PRIMARY KEY (organization, consumer_group_id, stream)
);
"#;

pub struct PgDatabase {
    pool: PgPool,
}

impl PgDatabase {
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(url)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        Self::with_pool(pool).await
    }

    pub async fn with_pool(pool: PgPool) -> Result<Self> {
        sqlx::raw_sql(SCHEMA_SQL)
            .execute(&pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> PgPool {
        self.pool.clone()
    }
}
