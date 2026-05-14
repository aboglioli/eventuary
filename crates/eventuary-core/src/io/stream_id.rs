use crate::error::{Error, Result};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct StreamId(String);

impl StreamId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() || value.len() > 128 {
            return Err(Error::Config(format!(
                "invalid stream id length: {:?}",
                value
            )));
        }
        if !value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
        {
            return Err(Error::Config(format!("invalid stream id: {value:?}")));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_id_validation() {
        assert!(StreamId::new("billing").is_ok());
        assert!(StreamId::new("billing.invoices").is_ok());
        assert!(StreamId::new("").is_err());
        assert!(StreamId::new("bad space").is_err());
    }
}
