use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FieldMap<V>(BTreeMap<String, V>);

impl<V> FieldMap<V> {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn with(mut self, key: impl Into<String>, value: V) -> Result<Self> {
        let key = key.into();
        Self::validate_key(&key)?;
        self.0.insert(key, value);
        Ok(self)
    }

    pub fn try_from_map(map: BTreeMap<String, V>) -> Result<Self> {
        for key in map.keys() {
            Self::validate_key(key)?;
        }
        Ok(Self(map))
    }

    pub fn get(&self, key: &str) -> Option<&V> {
        self.0.get(key)
    }

    pub fn has(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn as_map(&self) -> &BTreeMap<String, V> {
        &self.0
    }

    pub fn into_map(self) -> BTreeMap<String, V> {
        self.0
    }

    pub fn validate_key(key: &str) -> Result<()> {
        if key.is_empty() {
            return Err(Error::InvalidMetadataKey("must not be empty".to_owned()));
        }
        if key.len() > MAX_KEY_LEN {
            return Err(Error::InvalidMetadataKey(format!(
                "must be at most {MAX_KEY_LEN} characters"
            )));
        }
        let bytes = key.as_bytes();
        if !is_alphanumeric(bytes[0]) {
            return Err(Error::InvalidMetadataKey(
                "must start with an ASCII alphanumeric character".to_owned(),
            ));
        }
        if !is_alphanumeric(bytes[bytes.len() - 1]) {
            return Err(Error::InvalidMetadataKey(
                "must end with an ASCII alphanumeric character".to_owned(),
            ));
        }
        if bytes.len() < 3 {
            return Ok(());
        }
        for &byte in &bytes[1..bytes.len() - 1] {
            if !is_key_byte(byte) {
                return Err(Error::InvalidMetadataKey(format!(
                    "contains unsupported character: {:?}",
                    byte as char
                )));
            }
        }
        Ok(())
    }
}

const MAX_KEY_LEN: usize = 128;

fn is_alphanumeric(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
}

fn is_key_byte(byte: u8) -> bool {
    is_alphanumeric(byte) || matches!(byte, b'_' | b'-' | b'.' | b':' | b'/')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_inserts_valid_key() {
        let map: FieldMap<String> = FieldMap::new()
            .with("source", "billing".to_owned())
            .unwrap();
        assert_eq!(map.get("source"), Some(&"billing".to_owned()));
        assert!(map.has("source"));
        assert_eq!(map.len(), 1);
        assert!(!map.is_empty());
    }

    #[test]
    fn with_rejects_invalid_keys() {
        assert!(matches!(
            FieldMap::<String>::new().with("", "v".to_owned()),
            Err(Error::InvalidMetadataKey(_))
        ));
        assert!(matches!(
            FieldMap::<String>::new().with(" key", "v".to_owned()),
            Err(Error::InvalidMetadataKey(_))
        ));
        assert!(matches!(
            FieldMap::<String>::new().with("key ", "v".to_owned()),
            Err(Error::InvalidMetadataKey(_))
        ));
        assert!(matches!(
            FieldMap::<String>::new().with("k\nv", "v".to_owned()),
            Err(Error::InvalidMetadataKey(_))
        ));
        assert!(matches!(
            FieldMap::<String>::new().with("k\rv", "v".to_owned()),
            Err(Error::InvalidMetadataKey(_))
        ));
    }

    #[test]
    fn accepts_documented_valid_keys() {
        for key in [
            "handler_id",
            "error.kind",
            "http.status",
            "route/destination",
            "aws:receipt_handle",
            "retry-attempt",
            "a",
            "ABC123",
        ] {
            FieldMap::<String>::new()
                .with(key, "v".to_owned())
                .unwrap_or_else(|_| panic!("expected {key} to be valid"));
        }
    }

    #[test]
    fn rejects_keys_starting_or_ending_with_symbol() {
        for key in ["_handler_id", "handler_id_", ".kind", "kind.", "-x", "x-"] {
            assert!(
                matches!(
                    FieldMap::<String>::new().with(key, "v".to_owned()),
                    Err(Error::InvalidMetadataKey(_))
                ),
                "expected {key} to be rejected"
            );
        }
    }

    #[test]
    fn rejects_keys_with_unsupported_characters() {
        for key in ["handler id", "handler$id", "händler", "key\u{0007}id"] {
            assert!(
                matches!(
                    FieldMap::<String>::new().with(key, "v".to_owned()),
                    Err(Error::InvalidMetadataKey(_))
                ),
                "expected {key} to be rejected"
            );
        }
    }

    #[test]
    fn rejects_keys_exceeding_max_length() {
        let too_long = "a".repeat(129);
        assert!(matches!(
            FieldMap::<String>::new().with(&too_long, "v".to_owned()),
            Err(Error::InvalidMetadataKey(_))
        ));
        let max_ok = "a".repeat(128);
        FieldMap::<String>::new()
            .with(&max_ok, "v".to_owned())
            .unwrap();
    }

    #[test]
    fn try_from_map_validates_all_keys() {
        let mut valid = BTreeMap::new();
        valid.insert("a".to_owned(), 1u32);
        valid.insert("b".to_owned(), 2u32);
        let map = FieldMap::try_from_map(valid).unwrap();
        assert_eq!(map.len(), 2);
        assert_eq!(map.get("a"), Some(&1));

        let mut invalid = BTreeMap::new();
        invalid.insert("ok".to_owned(), 1u32);
        invalid.insert("".to_owned(), 2u32);
        assert!(matches!(
            FieldMap::try_from_map(invalid),
            Err(Error::InvalidMetadataKey(_))
        ));
    }
}
