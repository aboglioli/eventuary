use std::fmt;
use std::str::FromStr;

use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContentType {
    #[serde(rename = "application/json")]
    Json,
    #[serde(rename = "text/plain")]
    PlainText,
    #[serde(rename = "application/octet-stream")]
    Binary,
}

impl fmt::Display for ContentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ContentType::Json => write!(f, "application/json"),
            ContentType::PlainText => write!(f, "text/plain"),
            ContentType::Binary => write!(f, "application/octet-stream"),
        }
    }
}

impl FromStr for ContentType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "application/json" => Ok(ContentType::Json),
            "text/plain" => Ok(ContentType::PlainText),
            "application/octet-stream" => Ok(ContentType::Binary),
            other => Err(Error::InvalidPayload(format!(
                "unknown content type: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Payload {
    data: Bytes,
    content_type: ContentType,
}

impl Payload {
    pub fn from_json<T: Serialize>(val: &T) -> Result<Self> {
        let data = serde_json::to_vec(val).map_err(|e| Error::Serialization(e.to_string()))?;
        Ok(Self {
            data: Bytes::from(data),
            content_type: ContentType::Json,
        })
    }

    pub fn from_string(s: impl Into<String>) -> Self {
        Self {
            data: Bytes::from(s.into().into_bytes()),
            content_type: ContentType::PlainText,
        }
    }

    pub fn from_bytes(data: impl Into<Bytes>) -> Self {
        Self {
            data: data.into(),
            content_type: ContentType::Binary,
        }
    }

    pub fn from_raw(data: impl Into<Bytes>, content_type: ContentType) -> Self {
        Self {
            data: data.into(),
            content_type,
        }
    }

    pub fn to_json<T: DeserializeOwned>(&self) -> Result<T> {
        if self.content_type != ContentType::Json {
            return Err(Error::InvalidPayload("not JSON content".into()));
        }
        serde_json::from_slice(&self.data).map_err(|e| Error::Serialization(e.to_string()))
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn content_type(&self) -> ContentType {
        self.content_type
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_round_trip() {
        let payload = Payload::from_json(&serde_json::json!({"k": "v"})).unwrap();
        assert_eq!(payload.content_type(), ContentType::Json);
        let value: serde_json::Value = payload.to_json().unwrap();
        assert_eq!(value, serde_json::json!({"k": "v"}));
    }

    #[test]
    fn plain_text_payload() {
        let payload = Payload::from_string("hello");
        assert_eq!(payload.content_type(), ContentType::PlainText);
        assert_eq!(payload.data(), b"hello");
        assert_eq!(payload.size(), 5);
    }

    #[test]
    fn binary_payload_preserves_bytes() {
        let bytes = vec![0xff, 0x00, 0x01, 0xfe];
        let payload = Payload::from_bytes(bytes.clone());
        assert_eq!(payload.content_type(), ContentType::Binary);
        assert_eq!(payload.data(), bytes.as_slice());
    }

    #[test]
    fn from_bytes_accepts_static_slice() {
        let payload = Payload::from_bytes(Bytes::from_static(b"\x00\x01\x02"));
        assert_eq!(payload.data(), &[0x00, 0x01, 0x02]);
    }

    #[test]
    fn clone_does_not_copy_data() {
        let payload = Payload::from_bytes(vec![1u8; 1024]);
        let cloned = payload.clone();
        assert_eq!(payload.data().as_ptr(), cloned.data().as_ptr());
    }

    #[test]
    fn to_json_fails_on_non_json() {
        let payload = Payload::from_string("hello");
        let res: Result<serde_json::Value> = payload.to_json();
        assert!(matches!(res, Err(Error::InvalidPayload(_))));
    }

    #[test]
    fn content_type_round_trip() {
        for ct in [
            ContentType::Json,
            ContentType::PlainText,
            ContentType::Binary,
        ] {
            let s = ct.to_string();
            let parsed: ContentType = s.parse().unwrap();
            assert_eq!(parsed, ct);
        }
    }
}
