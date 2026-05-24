use std::fmt;
use std::sync::Arc;

use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OwnerId(Arc<str>);

impl OwnerId {
    pub fn new(value: impl Into<Arc<str>>) -> Result<Self> {
        let value: Arc<str> = value.into();
        if value.is_empty() || value.len() > 128 {
            return Err(Error::InvalidOwnerId(format!("{:?}", value.as_ref())));
        }
        if !value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.' || c == ':')
        {
            return Err(Error::InvalidOwnerId(format!("{:?}", value.as_ref())));
        }
        Ok(Self(value))
    }

    pub fn generate() -> Self {
        Self(Arc::from(uuid::Uuid::now_v7().to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for OwnerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl serde::Serialize for OwnerId {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&self.0)
    }
}

impl<'de> serde::Deserialize<'de> for OwnerId {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let value = String::deserialize(d)?;
        OwnerId::new(Arc::from(value)).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_accepts_representative_id() {
        let id = OwnerId::new("worker-01.primary:v2").unwrap();
        assert_eq!(id.as_str(), "worker-01.primary:v2");
    }

    #[test]
    fn new_rejects_empty() {
        assert!(OwnerId::new("").is_err());
    }

    #[test]
    fn new_rejects_too_long() {
        let long = "a".repeat(129);
        assert!(OwnerId::new(long).is_err());
    }

    #[test]
    fn new_accepts_boundary_128_chars() {
        let boundary = "a".repeat(128);
        assert!(OwnerId::new(boundary).is_ok());
    }

    #[test]
    fn new_rejects_invalid_charset() {
        assert!(OwnerId::new("bad space").is_err());
        assert!(OwnerId::new("bad/slash").is_err());
    }

    #[test]
    fn generate_produces_valid_value() {
        let id = OwnerId::generate();
        assert!(OwnerId::new(id.as_str()).is_ok());
    }

    #[test]
    fn generate_produces_unique_values() {
        let a = OwnerId::generate();
        let b = OwnerId::generate();
        assert_ne!(a, b);
    }

    #[test]
    fn serde_roundtrip() {
        let id = OwnerId::new("my-instance.1:v1").unwrap();
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"my-instance.1:v1\"");
        let back: OwnerId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, back);
    }
}
