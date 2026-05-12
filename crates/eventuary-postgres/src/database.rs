use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use eventuary_core::{Error, Result};

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
        sql.push_str(&format!(
            "INSERT INTO schema_migrations (version) VALUES ({}) ON CONFLICT (version) DO NOTHING;\n",
            migration.version()
        ));
    }
    sql
}

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
        apply_migrations(&pool).await?;

        Ok(Self { pool })
    }

    pub fn pool(&self) -> PgPool {
        self.pool.clone()
    }
}

async fn apply_migrations(pool: &PgPool) -> Result<()> {
    ensure_schema_migrations(pool).await?;
    let version = current_schema_version(pool).await?;
    for migration in migrations() {
        let migration_version = migration.version();
        if migration_version <= version {
            continue;
        }
        sqlx::raw_sql(migration.sql)
            .execute(pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        record_migration(pool, migration_version).await?;
    }
    Ok(())
}

async fn ensure_schema_migrations(pool: &PgPool) -> Result<()> {
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())",
    )
    .execute(pool)
    .await
    .map_err(|e| Error::Store(e.to_string()))?;
    Ok(())
}

async fn current_schema_version(pool: &PgPool) -> Result<i64> {
    let version: Option<i64> = sqlx::query_scalar(
        "SELECT version::bigint FROM schema_migrations ORDER BY version DESC LIMIT 1",
    )
    .fetch_optional(pool)
    .await
    .map_err(|e| Error::Store(e.to_string()))?;
    Ok(version.unwrap_or(0))
}

async fn record_migration(pool: &PgPool, version: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO schema_migrations (version) VALUES ($1) ON CONFLICT (version) DO NOTHING",
    )
    .bind(version)
    .execute(pool)
    .await
    .map_err(|e| Error::Store(e.to_string()))?;
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
        let sql: String = schema_sql();
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS events"));
        assert!(sql.contains("parent_id UUID"));
        assert!(sql.contains("event_key TEXT"));
        assert!(sql.contains("checkpoint_name"));
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS schema_migrations"));
        assert!(sql.contains("INSERT INTO schema_migrations (version) VALUES (1)"));
        assert!(sql.contains(migrations()[0].sql.trim()));
        assert_eq!(migrations()[0].filename, "0001_init.sql");
        assert_eq!(migrations()[0].version(), 1);
    }
}
