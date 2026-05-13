use eventuary_core::{Error, Result};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct SqliteRelationName {
    table: String,
}

impl SqliteRelationName {
    pub fn new(name: impl Into<String>) -> Result<Self> {
        let raw = name.into();
        validate_identifier(&raw)?;
        Ok(Self { table: raw })
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn render(&self) -> String {
        format!("\"{}\"", self.table)
    }
}

fn validate_identifier(identifier: &str) -> Result<()> {
    if identifier.is_empty() {
        return Err(Error::Config("empty sqlite identifier".to_owned()));
    }
    let mut chars = identifier.chars();
    let Some(first) = chars.next() else {
        return Err(Error::Config("empty sqlite identifier".to_owned()));
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(Error::Config(format!(
            "sqlite identifier must start with letter or underscore: {identifier:?}"
        )));
    }
    if !chars.all(|c| c == '_' || c.is_ascii_alphanumeric()) {
        return Err(Error::Config(format!(
            "sqlite identifier may only contain letters, digits, underscores: {identifier:?}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_simple_relation() {
        let relation = SqliteRelationName::new("events").unwrap();
        assert_eq!(relation.render(), "\"events\"");
    }

    #[test]
    fn rejects_invalid_relation() {
        assert!(SqliteRelationName::new("events;drop").is_err());
        assert!(SqliteRelationName::new("9events").is_err());
        assert!(SqliteRelationName::new("").is_err());
    }
}
