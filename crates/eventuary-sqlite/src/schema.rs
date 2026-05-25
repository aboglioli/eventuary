use rusqlite::Connection;

use eventuary_core::{Error, Result};

use crate::relation::SqliteRelationName;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Migration {
    pub name: &'static str,
    pub sql: &'static str,
}

#[derive(Clone, Copy, Debug)]
pub struct RelationReplacement<'a> {
    pub token: &'static str,
    pub relation: &'a SqliteRelationName,
}

pub fn render_migration_sql(
    migration: &Migration,
    replacements: &[RelationReplacement<'_>],
) -> String {
    let mut sql = migration.sql.to_owned();
    for replacement in replacements {
        sql = sql.replace(replacement.token, &replacement.relation.render());
    }
    sql
}

pub fn render_schema_sql(
    migrations: &[Migration],
    replacements: &[RelationReplacement<'_>],
) -> String {
    let mut sql = String::new();
    for migration in migrations {
        sql.push_str(&render_migration_sql(migration, replacements));
        if !sql.ends_with('\n') {
            sql.push('\n');
        }
    }
    sql
}

pub fn apply_schema(
    conn: &Connection,
    migrations: &[Migration],
    replacements: &[RelationReplacement<'_>],
) -> Result<()> {
    for migration in migrations {
        let sql = render_migration_sql(migration, replacements);
        for statement in sql.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            let result = conn.execute(statement, []);
            match result {
                Ok(_) => {}
                Err(ref e) if is_duplicate_add_column_error(statement, e) => {}
                Err(e) => return Err(Error::Store(format!("apply {}: {e}", migration.name))),
            }
        }
    }
    Ok(())
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
