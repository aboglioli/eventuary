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
        if key.trim() != key {
            return Err(Error::InvalidMetadataKey(
                "must not have leading or trailing whitespace".to_owned(),
            ));
        }
        if key.contains('\n') || key.contains('\r') {
            return Err(Error::InvalidMetadataKey(
                "must not contain newlines".to_owned(),
            ));
        }
        Ok(())
    }
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
