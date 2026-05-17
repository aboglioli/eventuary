use std::fmt;
use std::sync::Arc;

use crate::error::Result;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct CursorId(Arc<str>);

impl CursorId {
    pub fn new(value: impl Into<Arc<str>>) -> Result<Self> {
        let value: Arc<str> = value.into();
        if value.is_empty() || value.len() > 128 {
            return Err(crate::error::Error::Config(format!(
                "invalid cursor id: {:?}",
                value.as_ref()
            )));
        }
        if !value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.' || c == ':')
        {
            return Err(crate::error::Error::Config(format!(
                "invalid cursor id: {:?}",
                value.as_ref()
            )));
        }
        Ok(Self(value))
    }

    pub fn global() -> Self {
        Self(Arc::from("global"))
    }

    pub fn partition(count: u16, id: u16) -> Self {
        Self(Arc::from(format!("partition:{count}:{id}")))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CursorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl serde::Serialize for CursorId {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&self.0)
    }
}

impl<'de> serde::Deserialize<'de> for CursorId {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let value = String::deserialize(d)?;
        CursorId::new(Arc::from(value)).map_err(serde::de::Error::custom)
    }
}

pub trait Cursor {
    fn id(&self) -> CursorId {
        CursorId::global()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn global_returns_static_global_id() {
        assert_eq!(CursorId::global().as_str(), "global");
        assert_eq!(CursorId::global(), CursorId::global());
    }

    #[test]
    fn partition_constructs_stable_format() {
        let id = CursorId::partition(100, 17);
        assert_eq!(id.as_str(), "partition:100:17");
    }

    #[test]
    fn new_validates_and_stores_value() {
        let id = CursorId::new("custom.stream").unwrap();
        assert_eq!(id.as_str(), "custom.stream");
    }

    #[test]
    fn new_rejects_empty() {
        assert!(CursorId::new("").is_err());
    }

    #[test]
    fn new_rejects_too_long() {
        let long = "a".repeat(129);
        assert!(CursorId::new(long).is_err());
    }

    #[test]
    fn new_rejects_invalid_chars() {
        assert!(CursorId::new("bad space").is_err());
        assert!(CursorId::new("bad/char").is_err());
    }

    #[test]
    fn new_accepts_valid_chars() {
        assert!(CursorId::new("valid-name_01.v2:tag").is_ok());
    }

    #[test]
    fn equality_by_value() {
        let a = CursorId::new("test").unwrap();
        let b = CursorId::new("test").unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn distinct_values_differ() {
        let a = CursorId::new("a").unwrap();
        let b = CursorId::new("b").unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn cursor_trait_default_is_global() {
        struct SomeCursor;
        impl Cursor for SomeCursor {}
        assert_eq!(SomeCursor.id(), CursorId::global());
    }

    #[test]
    fn cursor_trait_named_example() {
        struct NamedCursor;
        impl Cursor for NamedCursor {
            fn id(&self) -> CursorId {
                CursorId::new("partition:100:17").unwrap()
            }
        }
        assert_eq!(NamedCursor.id(), CursorId::partition(100, 17));
    }

    #[test]
    fn serializes_as_plain_string() {
        let id = CursorId::global();
        let v = serde_json::to_value(id).unwrap();
        assert_eq!(v.as_str(), Some("global"));

        let id = CursorId::partition(4, 1);
        let v = serde_json::to_value(id).unwrap();
        assert_eq!(v.as_str(), Some("partition:4:1"));
    }

    #[test]
    fn roundtrips_via_json() {
        let id = CursorId::global();
        let v = serde_json::to_value(id.clone()).unwrap();
        let back: CursorId = serde_json::from_value(v).unwrap();
        assert_eq!(back, id);

        let id = CursorId::partition(4, 2);
        let v = serde_json::to_value(id.clone()).unwrap();
        let back: CursorId = serde_json::from_value(v).unwrap();
        assert_eq!(back, id);
    }

    #[test]
    fn display_output_matches_as_str() {
        let id = CursorId::global();
        assert_eq!(id.to_string(), "global");

        let id = CursorId::partition(4, 1);
        assert_eq!(id.to_string(), "partition:4:1");
    }
}
