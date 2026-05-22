use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::field_map::FieldMap;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Metadata(FieldMap<String>);

impl Metadata {
    pub fn new() -> Self {
        Self(FieldMap::new())
    }

    pub fn with(self, key: impl Into<String>, value: impl Into<String>) -> Result<Self> {
        Ok(Self(self.0.with(key, value.into())?))
    }

    pub fn from_map(map: BTreeMap<String, String>) -> Result<Self> {
        Ok(Self(FieldMap::try_from_map(map)?))
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(|s| s.as_str())
    }

    pub fn has(&self, key: &str) -> bool {
        self.0.has(key)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn as_map(&self) -> &BTreeMap<String, String> {
        self.0.as_map()
    }

    pub fn into_map(self) -> BTreeMap<String, String> {
        self.0.into_map()
    }
}

impl TryFrom<BTreeMap<String, String>> for Metadata {
    type Error = crate::error::Error;

    fn try_from(map: BTreeMap<String, String>) -> Result<Self> {
        Self::from_map(map)
    }
}

impl TryFrom<HashMap<String, String>> for Metadata {
    type Error = crate::error::Error;

    fn try_from(map: HashMap<String, String>) -> Result<Self> {
        Self::from_map(map.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;

    #[test]
    fn with_inserts_pair() {
        let m = Metadata::new().with("k", "v").unwrap();
        assert_eq!(m.get("k"), Some("v"));
        assert!(m.has("k"));
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn with_rejects_empty_key() {
        let res = Metadata::new().with("", "v");
        assert!(matches!(res, Err(Error::InvalidMetadataKey(_))));
    }

    #[test]
    fn with_rejects_whitespace_padded_key() {
        assert!(matches!(
            Metadata::new().with(" key", "v"),
            Err(Error::InvalidMetadataKey(_))
        ));
        assert!(matches!(
            Metadata::new().with("key ", "v"),
            Err(Error::InvalidMetadataKey(_))
        ));
    }

    #[test]
    fn with_rejects_newlines_in_key() {
        assert!(matches!(
            Metadata::new().with("k\nv", "v"),
            Err(Error::InvalidMetadataKey(_))
        ));
    }

    #[test]
    fn from_map_validates_keys() {
        let mut valid = BTreeMap::new();
        valid.insert("source".to_owned(), "billing".to_owned());
        let m = Metadata::from_map(valid).unwrap();
        assert_eq!(m.get("source"), Some("billing"));

        let mut invalid = BTreeMap::new();
        invalid.insert(String::new(), "v".to_owned());
        assert!(matches!(
            Metadata::from_map(invalid),
            Err(Error::InvalidMetadataKey(_))
        ));
    }

    #[test]
    fn try_from_hashmap_validates_keys() {
        let mut invalid = HashMap::new();
        invalid.insert(String::new(), "v".to_owned());
        assert!(matches!(
            Metadata::try_from(invalid),
            Err(Error::InvalidMetadataKey(_))
        ));
    }

    #[test]
    fn serializes_as_plain_map() {
        let m = Metadata::new().with("source", "billing").unwrap();
        let json = serde_json::to_string(&m).unwrap();
        assert_eq!(json, r#"{"source":"billing"}"#);
    }

    #[test]
    fn deserializes_from_plain_map() {
        let m: Metadata = serde_json::from_str(r#"{"source":"billing"}"#).unwrap();
        assert_eq!(m.get("source"), Some("billing"));
    }
}
