use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Metadata(HashMap<String, String>);

impl Metadata {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn with(mut self, key: impl Into<String>, value: impl Into<String>) -> Result<Self> {
        let key = key.into();
        Self::validate_key(&key)?;
        self.0.insert(key, value.into());
        Ok(self)
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(|s| s.as_str())
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

    pub fn as_map(&self) -> &HashMap<String, String> {
        &self.0
    }

    pub fn into_map(self) -> HashMap<String, String> {
        self.0
    }

    fn validate_key(key: &str) -> Result<()> {
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

impl From<HashMap<String, String>> for Metadata {
    fn from(map: HashMap<String, String>) -> Self {
        Self(map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn from_map_skips_validation() {
        let mut map = HashMap::new();
        map.insert(String::new(), "v".to_owned());
        let m = Metadata::from(map);
        assert_eq!(m.len(), 1);
    }
}
