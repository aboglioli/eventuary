use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use eventuary_core::{Error, Result};

use crate::relation::PgRelationName;

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
        filename: "0006_buffer_claims.sql",
        template: include_str!("../migrations/0006_buffer_claims.sql"),
    },
];

pub fn migrations() -> &'static [Migration] {
    MIGRATION_TEMPLATES
}

#[derive(Debug, Clone)]
pub struct PgDatabaseConfig {
    pub events_relation: PgRelationName,
    pub offsets_relation: PgRelationName,
    pub multiplexer_completions_relation: PgRelationName,
    pub dedupe_keys_relation: PgRelationName,
    pub buffer_entries_relation: PgRelationName,
    pub watermarks_relation: PgRelationName,
    pub consumers_relation: PgRelationName,
    pub partitions_relation: PgRelationName,
    pub buffer_claims_relation: PgRelationName,
    pub max_connections: u32,
}

impl Default for PgDatabaseConfig {
    fn default() -> Self {
        Self {
            events_relation: PgRelationName::new("events").expect("default events relation"),
            offsets_relation: PgRelationName::new("consumer_offsets")
                .expect("default offsets relation"),
            multiplexer_completions_relation: PgRelationName::new("multiplexer_completions")
                .expect("default multiplexer relation"),
            dedupe_keys_relation: PgRelationName::new("dedupe_keys")
                .expect("default dedupe relation"),
            buffer_entries_relation: PgRelationName::new("buffer_entries")
                .expect("default buffer relation"),
            watermarks_relation: PgRelationName::new("watermarks")
                .expect("default watermarks relation"),
            consumers_relation: PgRelationName::new("event_stream_consumers")
                .expect("default consumers relation"),
            partitions_relation: PgRelationName::new("event_stream_partitions")
                .expect("default partitions relation"),
            buffer_claims_relation: PgRelationName::new("event_buffer_claims")
                .expect("default buffer claims relation"),
            max_connections: 20,
        }
    }
}

impl PgDatabaseConfig {
    pub fn with_schema(schema: impl AsRef<str>) -> Result<Self> {
        let schema = schema.as_ref();
        Ok(Self {
            events_relation: PgRelationName::new(format!("{schema}.events"))?,
            offsets_relation: PgRelationName::new(format!("{schema}.consumer_offsets"))?,
            multiplexer_completions_relation: PgRelationName::new(format!(
                "{schema}.multiplexer_completions"
            ))?,
            dedupe_keys_relation: PgRelationName::new(format!("{schema}.dedupe_keys"))?,
            buffer_entries_relation: PgRelationName::new(format!("{schema}.buffer_entries"))?,
            watermarks_relation: PgRelationName::new(format!("{schema}.watermarks"))?,
            consumers_relation: PgRelationName::new(format!("{schema}.event_stream_consumers"))?,
            partitions_relation: PgRelationName::new(format!("{schema}.event_stream_partitions"))?,
            buffer_claims_relation: PgRelationName::new(format!("{schema}.event_buffer_claims"))?,
            max_connections: 20,
        })
    }
}

pub fn render_migration_sql(migration: &Migration, config: &PgDatabaseConfig) -> String {
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
        .replace("{buffer_claims}", &config.buffer_claims_relation.render())
}

pub fn render_schema_sql(config: &PgDatabaseConfig) -> String {
    let mut sql = String::new();
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for relation in [
        &config.events_relation,
        &config.offsets_relation,
        &config.multiplexer_completions_relation,
        &config.dedupe_keys_relation,
        &config.buffer_entries_relation,
        &config.watermarks_relation,
        &config.consumers_relation,
        &config.partitions_relation,
        &config.buffer_claims_relation,
    ] {
        if let Some(schema) = relation.schema()
            && seen.insert(schema)
        {
            sql.push_str(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\";\n"));
        }
    }
    for migration in migrations() {
        sql.push_str(&render_migration_sql(migration, config));
        if !sql.ends_with('\n') {
            sql.push('\n');
        }
    }
    sql
}

pub fn schema_sql() -> String {
    render_schema_sql(&PgDatabaseConfig::default())
}

#[derive(Clone, Debug)]
pub struct PgConnectOptions {
    pub max_connections: u32,
}

impl Default for PgConnectOptions {
    fn default() -> Self {
        Self {
            max_connections: 20,
        }
    }
}

pub struct PgDatabase {
    pool: PgPool,
    config: PgDatabaseConfig,
}

impl PgDatabase {
    pub async fn connect(url: &str) -> Result<Self> {
        Self::connect_with(url, PgConnectOptions::default()).await
    }

    pub async fn connect_with(url: &str, options: PgConnectOptions) -> Result<Self> {
        let config = PgDatabaseConfig {
            max_connections: options.max_connections,
            ..PgDatabaseConfig::default()
        };
        Self::connect_with_config(url, config).await
    }

    pub async fn connect_with_config(url: &str, config: PgDatabaseConfig) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(url)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        Self::with_pool_and_config(pool, config).await
    }

    pub async fn with_pool(pool: PgPool) -> Result<Self> {
        Self::with_pool_and_config(pool, PgDatabaseConfig::default()).await
    }

    pub async fn with_pool_and_config(pool: PgPool, config: PgDatabaseConfig) -> Result<Self> {
        apply_migrations(&pool, &config).await?;
        Ok(Self { pool, config })
    }

    pub fn pool(&self) -> PgPool {
        self.pool.clone()
    }

    pub fn config(&self) -> &PgDatabaseConfig {
        &self.config
    }
}

async fn apply_migrations(pool: &PgPool, config: &PgDatabaseConfig) -> Result<()> {
    let schemas: Vec<&str> = [
        config.events_relation.schema(),
        config.offsets_relation.schema(),
        config.multiplexer_completions_relation.schema(),
        config.dedupe_keys_relation.schema(),
        config.buffer_entries_relation.schema(),
        config.watermarks_relation.schema(),
        config.consumers_relation.schema(),
        config.partitions_relation.schema(),
        config.buffer_claims_relation.schema(),
    ]
    .into_iter()
    .flatten()
    .collect();
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for schema in schemas {
        if seen.insert(schema) {
            sqlx::raw_sql(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\""))
                .execute(pool)
                .await
                .map_err(|e| Error::Store(e.to_string()))?;
        }
    }
    // Every statement in the rendered migration is `CREATE TABLE IF NOT
    // EXISTS` / `CREATE INDEX IF NOT EXISTS` / `ALTER TABLE ... IF NOT
    // EXISTS`, so it is safe to apply unconditionally on every connect.
    // This avoids skipping configured-relation creation when an earlier
    // process recorded a global migration version under different
    // relation names.
    for migration in migrations() {
        let sql = render_migration_sql(migration, config);
        sqlx::raw_sql(&sql)
            .execute(pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
    }
    Ok(())
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
    fn schema_sql_is_available_for_manual_migrations() {
        let sql = schema_sql();
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"events\""));
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"consumer_offsets\""));
        assert!(sql.contains("parent_id UUID"));
        assert!(sql.contains("event_key TEXT"));
        assert!(sql.contains("stream_id"));
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"events\""));
    }

    #[test]
    fn schema_sql_uses_configured_relations() {
        let config = PgDatabaseConfig::with_schema("eventuary").unwrap();
        let sql = render_schema_sql(&config);
        assert!(sql.contains("CREATE SCHEMA IF NOT EXISTS \"eventuary\""));
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"eventuary\".\"events\""));
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"eventuary\".\"consumer_offsets\""));
    }
}
