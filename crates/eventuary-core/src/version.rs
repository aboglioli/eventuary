use std::fmt;
use std::num::{NonZeroU32, NonZeroU64};

use serde::{Deserialize, Serialize};

use crate::collector::AGGREGATE_VERSION_KEY;
use crate::error::{Error, Result};
use crate::event::Event;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct SchemaVersion(NonZeroU32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct SpecVersion(NonZeroU32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct AggregateVersion(NonZeroU64);

impl SchemaVersion {
    pub const FIRST: Self = Self(NonZeroU32::MIN);

    pub fn new(value: u32) -> Result<Self> {
        NonZeroU32::new(value)
            .map(Self)
            .ok_or_else(|| Error::InvalidVersion("schema version must be at least 1".to_owned()))
    }

    pub fn value(self) -> u32 {
        self.0.get()
    }
}

impl SpecVersion {
    pub const V1: Self = Self(NonZeroU32::MIN);
    pub const CURRENT: Self = Self::V1;

    pub fn new(value: u32) -> Result<Self> {
        let Some(version) = NonZeroU32::new(value) else {
            return Err(Error::InvalidVersion(
                "spec version must be at least 1".to_owned(),
            ));
        };
        if version > Self::CURRENT.0 {
            return Err(Error::InvalidVersion(format!(
                "spec version {value} is newer than {}, the latest this build understands",
                Self::CURRENT.0
            )));
        }
        Ok(Self(version))
    }

    pub fn value(self) -> u32 {
        self.0.get()
    }
}

impl AggregateVersion {
    pub const FIRST: Self = Self(NonZeroU64::MIN);

    pub fn new(value: u64) -> Result<Self> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or_else(|| Error::InvalidVersion("aggregate version must be at least 1".to_owned()))
    }

    pub fn of<P>(event: &Event<P>) -> Result<Option<Self>> {
        let Some(value) = event.metadata().get(AGGREGATE_VERSION_KEY) else {
            return Ok(None);
        };
        let value = value.parse::<u64>().map_err(|_| {
            Error::InvalidVersion(format!("aggregate version is not a number: {value}"))
        })?;
        Self::new(value).map(Some)
    }

    pub fn next(self) -> Result<Self> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or_else(|| Error::InvalidVersion("aggregate version overflowed".to_owned()))
    }

    pub fn value(self) -> u64 {
        self.0.get()
    }
}

impl TryFrom<u64> for AggregateVersion {
    type Error = Error;

    fn try_from(value: u64) -> Result<Self> {
        Self::new(value)
    }
}

impl From<AggregateVersion> for u64 {
    fn from(version: AggregateVersion) -> Self {
        version.value()
    }
}

impl fmt::Display for AggregateVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Default for SchemaVersion {
    fn default() -> Self {
        Self::FIRST
    }
}

impl Default for SpecVersion {
    fn default() -> Self {
        Self::CURRENT
    }
}

impl TryFrom<u32> for SchemaVersion {
    type Error = Error;

    fn try_from(value: u32) -> Result<Self> {
        Self::new(value)
    }
}

impl TryFrom<u32> for SpecVersion {
    type Error = Error;

    fn try_from(value: u32) -> Result<Self> {
        Self::new(value)
    }
}

impl From<SchemaVersion> for u32 {
    fn from(version: SchemaVersion) -> Self {
        version.value()
    }
}

impl From<SpecVersion> for u32 {
    fn from(version: SpecVersion) -> Self {
        version.value()
    }
}

impl fmt::Display for SchemaVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for SpecVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregate_version_starts_at_one_and_advances() {
        assert!(AggregateVersion::new(0).is_err());
        assert_eq!(AggregateVersion::FIRST.next().unwrap().value(), 2);
        assert!(AggregateVersion::new(u64::MAX).unwrap().next().is_err());
    }

    #[test]
    fn schema_version_starts_at_one() {
        assert!(SchemaVersion::new(0).is_err());
        assert_eq!(SchemaVersion::new(3).unwrap().value(), 3);
        assert_eq!(SchemaVersion::default(), SchemaVersion::FIRST);
    }

    #[test]
    fn spec_version_rejects_zero_and_unknown_future_versions() {
        assert!(SpecVersion::new(0).is_err());
        assert!(SpecVersion::new(SpecVersion::CURRENT.value() + 1).is_err());
        assert_eq!(SpecVersion::new(1).unwrap(), SpecVersion::V1);
        assert_eq!(SpecVersion::default(), SpecVersion::CURRENT);
    }

    #[test]
    fn versions_serialize_as_plain_numbers() {
        assert_eq!(
            serde_json::to_value(SchemaVersion::new(2).unwrap()).unwrap(),
            2
        );
        let parsed: SpecVersion = serde_json::from_value(serde_json::json!(1)).unwrap();
        assert_eq!(parsed, SpecVersion::V1);
        assert!(serde_json::from_value::<SchemaVersion>(serde_json::json!(0)).is_err());
    }
}
