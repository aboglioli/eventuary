use std::collections::HashSet;

use sqlx::PgPool;

use eventuary_core::{Error, Result};

use crate::relation::PgRelationName;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Migration {
    pub(crate) name: &'static str,
    pub(crate) sql: &'static str,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RelationReplacement<'a> {
    pub(crate) token: &'static str,
    pub(crate) relation: &'a PgRelationName,
}

pub(crate) fn render_migration_sql(
    migration: &Migration,
    replacements: &[RelationReplacement<'_>],
) -> String {
    let mut sql = migration.sql.to_owned();
    for replacement in replacements {
        sql = sql.replace(replacement.token, &replacement.relation.render());
    }
    sql
}

pub(crate) fn render_schema_sql(
    migrations: &[Migration],
    replacements: &[RelationReplacement<'_>],
) -> String {
    let mut sql = render_schema_creation_sql(replacements);
    for migration in migrations {
        sql.push_str(&render_migration_sql(migration, replacements));
        if !sql.ends_with('\n') {
            sql.push('\n');
        }
    }
    sql
}

fn render_schema_creation_sql(replacements: &[RelationReplacement<'_>]) -> String {
    let mut sql = String::new();
    let mut seen = HashSet::new();
    for replacement in replacements {
        let Some(schema) = replacement.relation.schema() else {
            continue;
        };
        if seen.insert(schema) {
            sql.push_str(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\";\n"));
        }
    }
    sql
}

pub(crate) async fn apply_schema(
    pool: &PgPool,
    migrations: &[Migration],
    replacements: &[RelationReplacement<'_>],
) -> Result<()> {
    create_schemas(pool, replacements).await?;
    for migration in migrations {
        let sql = render_migration_sql(migration, replacements);
        sqlx::raw_sql(&sql)
            .execute(pool)
            .await
            .map_err(|e| Error::Store(format!("apply {}: {e}", migration.name)))?;
    }
    Ok(())
}

async fn create_schemas(pool: &PgPool, replacements: &[RelationReplacement<'_>]) -> Result<()> {
    let mut seen = HashSet::new();
    for replacement in replacements {
        let Some(schema) = replacement.relation.schema() else {
            continue;
        };
        if !seen.insert(schema) {
            continue;
        }
        sqlx::raw_sql(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\""))
            .execute(pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
    }
    Ok(())
}
