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

const MIGRATION_TEMPLATES: &[Migration] = &[Migration {
    filename: "0001_init.sql",
    template: include_str!("../migrations/0001_init.sql"),
}];

pub fn migrations() -> &'static [Migration] {
    MIGRATION_TEMPLATES
}

#[derive(Debug, Clone)]
pub struct PgDatabaseConfig {
    pub events_relation: PgRelationName,
    pub offsets_relation: PgRelationName,
    pub max_connections: u32,
}

impl Default for PgDatabaseConfig {
    fn default() -> Self {
        Self {
            events_relation: PgRelationName::new("events").expect("default events relation"),
            offsets_relation: PgRelationName::new("consumer_offsets")
                .expect("default offsets relation"),
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
            max_connections: 20,
        })
    }
}

pub fn render_migration_sql(migration: &Migration, config: &PgDatabaseConfig) -> String {
    migration
        .template
        .replace("{events}", &config.events_relation.render())
        .replace("{offsets}", &config.offsets_relation.render())
}

pub fn render_schema_sql(config: &PgDatabaseConfig) -> String {
    let mut sql = String::new();
    if let Some(schema) = config.events_relation.schema() {
        sql.push_str(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\";\n"));
    }
    if let Some(schema) = config.offsets_relation.schema()
        && Some(schema) != config.events_relation.schema()
    {
        sql.push_str(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\";\n"));
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
    if let Some(schema) = config.events_relation.schema() {
        sqlx::raw_sql(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\""))
            .execute(pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
    }
    if let Some(schema) = config.offsets_relation.schema()
        && Some(schema) != config.events_relation.schema()
    {
        sqlx::raw_sql(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\""))
            .execute(pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
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
