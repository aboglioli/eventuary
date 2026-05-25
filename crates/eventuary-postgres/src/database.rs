use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use eventuary_core::{Error, Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgDatabaseConfig {
    pub max_connections: u32,
}

impl Default for PgDatabaseConfig {
    fn default() -> Self {
        Self {
            max_connections: 20,
        }
    }
}

pub type PgConnectOptions = PgDatabaseConfig;

pub struct PgDatabase {
    pool: PgPool,
    config: PgDatabaseConfig,
}

impl PgDatabase {
    pub async fn connect(url: &str) -> Result<Self> {
        Self::connect_with_config(url, PgDatabaseConfig::default()).await
    }

    pub async fn connect_with(url: &str, options: PgConnectOptions) -> Result<Self> {
        Self::connect_with_config(url, options).await
    }

    pub async fn connect_with_config(url: &str, config: PgDatabaseConfig) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(url)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        Ok(Self { pool, config })
    }

    pub fn with_pool(pool: PgPool) -> Self {
        Self::with_pool_and_config(pool, PgDatabaseConfig::default())
    }

    pub fn with_pool_and_config(pool: PgPool, config: PgDatabaseConfig) -> Self {
        Self { pool, config }
    }

    pub fn pool(&self) -> PgPool {
        self.pool.clone()
    }

    pub fn config(&self) -> &PgDatabaseConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_uses_twenty_connections() {
        assert_eq!(PgDatabaseConfig::default().max_connections, 20);
    }

    #[test]
    fn connect_options_aliases_database_config() {
        let options = PgConnectOptions { max_connections: 7 };
        assert_eq!(options.max_connections, 7);
    }
}
