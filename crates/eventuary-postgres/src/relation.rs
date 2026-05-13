use eventuary_core::{Error, Result};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct PgRelationName {
    schema: Option<String>,
    table: String,
}

impl PgRelationName {
    pub fn new(name: impl Into<String>) -> Result<Self> {
        let raw = name.into();
        let parts: Vec<&str> = raw.split('.').collect();
        match parts.len() {
            1 => {
                validate_identifier(parts[0])?;
                Ok(Self {
                    schema: None,
                    table: parts[0].to_owned(),
                })
            }
            2 => {
                validate_identifier(parts[0])?;
                validate_identifier(parts[1])?;
                Ok(Self {
                    schema: Some(parts[0].to_owned()),
                    table: parts[1].to_owned(),
                })
            }
            _ => Err(Error::Config(format!(
                "invalid postgres relation name: {raw}"
            ))),
        }
    }

    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn render(&self) -> String {
        match &self.schema {
            Some(schema) => format!("\"{}\".\"{}\"", schema, self.table),
            None => format!("\"{}\"", self.table),
        }
    }
}

fn validate_identifier(identifier: &str) -> Result<()> {
    if identifier.is_empty() || identifier.len() > 63 {
        return Err(Error::Config(format!(
            "invalid postgres identifier length: {identifier:?}"
        )));
    }
    let mut chars = identifier.chars();
    let Some(first) = chars.next() else {
        return Err(Error::Config("empty postgres identifier".to_owned()));
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(Error::Config(format!(
            "postgres identifier must start with letter or underscore: {identifier:?}"
        )));
    }
    if !chars.all(|c| c == '_' || c.is_ascii_alphanumeric()) {
        return Err(Error::Config(format!(
            "postgres identifier may only contain letters, digits, underscores: {identifier:?}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_simple_relation() {
        let relation = PgRelationName::new("events").unwrap();
        assert_eq!(relation.render(), "\"events\"");
    }

    #[test]
    fn renders_schema_qualified_relation() {
        let relation = PgRelationName::new("eventuary.events").unwrap();
        assert_eq!(relation.render(), "\"eventuary\".\"events\"");
    }

    #[test]
    fn rejects_invalid_relation() {
        assert!(PgRelationName::new("events;drop").is_err());
        assert!(PgRelationName::new("public.events.extra").is_err());
        assert!(PgRelationName::new("9events").is_err());
        assert!(PgRelationName::new("").is_err());
    }
}
