use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ConsumerGroupId(String);

impl ConsumerGroupId {
    pub fn new(s: impl Into<String>) -> Result<Self> {
        let s = s.into();
        if s.is_empty() {
            return Err(Error::InvalidConsumerGroupId("must not be empty".into()));
        }
        if s.len() > 64 {
            return Err(Error::InvalidConsumerGroupId("max 64 chars".into()));
        }
        if !s
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '-' | '_' | '.'))
        {
            return Err(Error::InvalidConsumerGroupId(format!("invalid: {s}")));
        }
        let is_edge = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit();
        if !s.starts_with(is_edge) || !s.ends_with(is_edge) {
            return Err(Error::InvalidConsumerGroupId(format!(
                "must start and end with a lowercase letter or digit: {s}"
            )));
        }
        if s.contains("..") {
            return Err(Error::InvalidConsumerGroupId(format!(
                "must not contain consecutive dots: {s}"
            )));
        }
        Ok(Self(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ConsumerGroupId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<String> for ConsumerGroupId {
    type Error = Error;
    fn try_from(s: String) -> Result<Self> {
        Self::new(s)
    }
}

impl From<ConsumerGroupId> for String {
    fn from(g: ConsumerGroupId) -> Self {
        g.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid() {
        assert!(ConsumerGroupId::new("heartbeat-monitor").is_ok());
        assert!(ConsumerGroupId::new("group_1").is_ok());
        assert!(ConsumerGroupId::new("g").is_ok());
    }

    #[test]
    fn invalid_empty() {
        assert!(ConsumerGroupId::new("").is_err());
    }

    #[test]
    fn invalid_too_long() {
        let s = "a".repeat(65);
        assert!(ConsumerGroupId::new(s).is_err());
    }

    #[test]
    fn invalid_charset() {
        assert!(ConsumerGroupId::new("Heartbeat").is_err());
        assert!(ConsumerGroupId::new("group/1").is_err());
    }

    #[test]
    fn dots_separate_words() {
        assert!(ConsumerGroupId::new("group.1").is_ok());
        assert!(ConsumerGroupId::new("identity.send_email_verification").is_ok());
    }

    #[test]
    fn dots_cannot_form_a_relative_path() {
        assert!(ConsumerGroupId::new(".").is_err());
        assert!(ConsumerGroupId::new("..").is_err());
        assert!(ConsumerGroupId::new(".hidden").is_err());
        assert!(ConsumerGroupId::new("trailing.").is_err());
        assert!(ConsumerGroupId::new("a..b").is_err());
    }

    #[test]
    fn edges_are_letters_or_digits() {
        assert!(ConsumerGroupId::new("-group").is_err());
        assert!(ConsumerGroupId::new("group_").is_err());
        assert!(ConsumerGroupId::new("g").is_ok());
    }

    #[test]
    fn boundary_64_chars() {
        let s = "a".repeat(64);
        assert!(ConsumerGroupId::new(s).is_ok());
    }

    #[test]
    fn try_from_string_roundtrip() {
        let g = ConsumerGroupId::new("my-group").unwrap();
        let s: String = g.clone().into();
        let g2 = ConsumerGroupId::try_from(s).unwrap();
        assert_eq!(g, g2);
    }
}
