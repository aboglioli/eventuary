use std::collections::HashMap;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::event::{Event, EventId};
use crate::event_key::EventKey;
use crate::metadata::Metadata;
use crate::namespace::Namespace;
use crate::organization::OrganizationId;
use crate::payload::{ContentType, Payload};
use crate::topic::Topic;

/// Wire-format representation of an [`Event`]. Field order matches
/// `Event` so the JSON shape is predictable and self-documenting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedEvent {
    pub id: String,
    pub organization: String,
    pub namespace: String,
    pub topic: String,
    pub payload: SerializedPayload,
    pub metadata: HashMap<String, String>,
    pub timestamp: DateTime<Utc>,
    pub version: u64,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub parent_id: Option<String>,
    #[serde(default)]
    pub correlation_id: Option<String>,
    #[serde(default)]
    pub causation_id: Option<String>,
}

/// Wire-format representation of a [`Payload`].
///
/// Each variant carries the payload bytes in the natural shape for its
/// content type:
/// - `Json` carries the parsed `serde_json::Value` so the wire format
///   stays human-readable and `jq`-friendly.
/// - `PlainText` carries the text as a raw JSON string.
/// - `Binary` carries the raw bytes base64-encoded.
///
/// The wire tag is the content-type string, so the JSON shape is
/// `{"content_type": "...", "data": ...}` regardless of variant.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "content_type", content = "data")]
pub enum SerializedPayload {
    #[serde(rename = "application/json")]
    Json(serde_json::Value),
    #[serde(rename = "text/plain")]
    PlainText(String),
    #[serde(rename = "application/octet-stream")]
    Binary(#[serde(with = "base64_bytes")] Vec<u8>),
}

impl SerializedPayload {
    pub fn content_type(&self) -> ContentType {
        match self {
            Self::Json(_) => ContentType::Json,
            Self::PlainText(_) => ContentType::PlainText,
            Self::Binary(_) => ContentType::Binary,
        }
    }

    pub fn from_payload(payload: &Payload) -> Result<Self> {
        match payload.content_type() {
            ContentType::Json => {
                let value = serde_json::from_slice(payload.data())
                    .map_err(|e| Error::Serialization(e.to_string()))?;
                Ok(Self::Json(value))
            }
            ContentType::PlainText => {
                let text = std::str::from_utf8(payload.data())
                    .map_err(|e| Error::Serialization(e.to_string()))?;
                Ok(Self::PlainText(text.to_owned()))
            }
            ContentType::Binary => Ok(Self::Binary(payload.data().to_vec())),
        }
    }

    pub fn into_payload(self) -> Result<Payload> {
        match self {
            Self::Json(value) => {
                let bytes =
                    serde_json::to_vec(&value).map_err(|e| Error::Serialization(e.to_string()))?;
                Ok(Payload::from_raw(bytes, ContentType::Json))
            }
            Self::PlainText(text) => Ok(Payload::from_string(text)),
            Self::Binary(bytes) => Ok(Payload::from_bytes(bytes)),
        }
    }
}

mod base64_bytes {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64;
    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S: Serializer>(bytes: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&BASE64.encode(bytes))
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let encoded = String::deserialize(d)?;
        BASE64.decode(encoded).map_err(serde::de::Error::custom)
    }
}

impl SerializedEvent {
    pub fn from_event(event: &Event) -> Result<Self> {
        Ok(Self {
            id: event.id().to_string(),
            organization: event.organization().to_string(),
            namespace: event.namespace().to_string(),
            topic: event.topic().to_string(),
            payload: SerializedPayload::from_payload(event.payload())?,
            metadata: event.metadata().as_map().clone(),
            timestamp: event.timestamp(),
            version: event.version(),
            key: event.key().map(|key| key.to_string()),
            parent_id: event.parent_id().map(|id| id.to_string()),
            correlation_id: event.correlation_id().map(|id| id.to_string()),
            causation_id: event.causation_id().map(|id| id.to_string()),
        })
    }

    pub fn to_event(&self) -> Result<Event> {
        let payload = self.payload.clone().into_payload()?;
        let key = self.key.as_deref().map(EventKey::new).transpose()?;
        let parent_id = self
            .parent_id
            .as_deref()
            .map(EventId::from_str)
            .transpose()
            .map_err(|e| Error::Serialization(e.to_string()))?;
        let correlation_id = self
            .correlation_id
            .as_deref()
            .map(EventKey::new)
            .transpose()?;
        let causation_id = self
            .causation_id
            .as_deref()
            .map(EventKey::new)
            .transpose()?;

        Event::new(
            EventId::from_str(&self.id).map_err(|e| Error::Serialization(e.to_string()))?,
            OrganizationId::new(&self.organization)?,
            Namespace::new(&self.namespace)?,
            Topic::new(&self.topic)?,
            payload,
            Metadata::from(self.metadata.clone()),
            self.timestamp,
            self.version,
            key,
            parent_id,
            correlation_id,
            causation_id,
        )
    }

    pub fn to_json_value(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("SerializedEvent must serialize to JSON")
    }

    pub fn from_json_value(value: serde_json::Value) -> Result<Self> {
        serde_json::from_value(value).map_err(|e| Error::Serialization(e.to_string()))
    }

    pub fn to_json_string(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|e| Error::Serialization(e.to_string()))
    }

    pub fn from_json_str(s: &str) -> Result<Self> {
        serde_json::from_str(s).map_err(|e| Error::Serialization(e.to_string()))
    }

    pub fn from_json_slice(bytes: &[u8]) -> Result<Self> {
        let value =
            serde_json::from_slice(bytes).map_err(|e| Error::Serialization(e.to_string()))?;
        Self::from_json_value(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event_with_key(payload: Payload, key: &str) -> Event {
        Event::builder("acme", "/x", "thing.happened", payload)
            .unwrap()
            .key(key)
            .unwrap()
            .build()
            .expect("valid event")
    }

    #[test]
    fn roundtrip() {
        let payload = Payload::from_json(&serde_json::json!({"key": "value"})).unwrap();
        let event = Event::builder("acme", "/task", "task.created", payload)
            .unwrap()
            .key("task-123")
            .unwrap()
            .build()
            .unwrap();

        let serialized = SerializedEvent::from_event(&event).unwrap();
        assert_eq!(serialized.topic, "task.created");
        assert_eq!(serialized.namespace, "/task");
        assert_eq!(serialized.organization, "acme");
        assert_eq!(serialized.key.as_deref(), Some("task-123"));

        let restored = serialized.to_event().unwrap();
        assert_eq!(restored.topic().as_str(), "task.created");
        assert_eq!(restored.id(), event.id());
        assert_eq!(restored.key().map(EventKey::as_str), Some("task-123"));
    }

    #[test]
    fn field_order_matches_event() {
        let event = Event::builder("acme", "/x", "thing.happened", Payload::from_string("p"))
            .unwrap()
            .key("k")
            .unwrap()
            .build()
            .unwrap();
        let serialized = SerializedEvent::from_event(&event).unwrap();
        let json = serialized.to_json_string().unwrap();
        let id_pos = json.find("\"id\"").unwrap();
        let org_pos = json.find("\"organization\"").unwrap();
        let ns_pos = json.find("\"namespace\"").unwrap();
        let topic_pos = json.find("\"topic\"").unwrap();
        let payload_pos = json.find("\"payload\"").unwrap();
        let metadata_pos = json.find("\"metadata\"").unwrap();
        let timestamp_pos = json.find("\"timestamp\"").unwrap();
        let version_pos = json.find("\"version\"").unwrap();
        let key_pos = json.find("\"key\"").unwrap();
        assert!(id_pos < org_pos);
        assert!(org_pos < ns_pos);
        assert!(ns_pos < topic_pos);
        assert!(topic_pos < payload_pos);
        assert!(payload_pos < metadata_pos);
        assert!(metadata_pos < timestamp_pos);
        assert!(timestamp_pos < version_pos);
        assert!(version_pos < key_pos);
    }

    #[test]
    fn optional_key_can_be_absent() {
        let event = Event::create(
            "acme",
            "/x",
            "thing.happened",
            Payload::from_string("no-key"),
        )
        .unwrap();
        let serialized = SerializedEvent::from_event(&event).unwrap();
        assert_eq!(serialized.key, None);
        let restored = serialized.to_event().unwrap();
        assert_eq!(restored.key(), None);
    }

    #[test]
    fn plain_text_payload_is_a_raw_json_string() {
        let event = event_with_key(Payload::from_string("hello world"), "k");
        let serialized = SerializedEvent::from_event(&event).unwrap();
        match &serialized.payload {
            SerializedPayload::PlainText(text) => assert_eq!(text, "hello world"),
            other => panic!("expected PlainText, got {other:?}"),
        }
        let json = serialized.to_json_string().unwrap();
        assert!(json.contains("\"content_type\":\"text/plain\""));
        assert!(json.contains("\"data\":\"hello world\""));

        let restored = serialized.to_event().unwrap();
        assert_eq!(restored.payload().data(), b"hello world");
        assert_eq!(restored.payload().content_type(), ContentType::PlainText);
    }

    #[test]
    fn binary_payload_is_base64_in_data_field() {
        let bytes = vec![0xff, 0x00, 0x01, 0xfe, 0x80, 0x7f, 0x10];
        let event = event_with_key(Payload::from_bytes(bytes.clone()), "k");
        let serialized = SerializedEvent::from_event(&event).unwrap();
        match &serialized.payload {
            SerializedPayload::Binary(b) => assert_eq!(b, &bytes),
            other => panic!("expected Binary, got {other:?}"),
        }
        let json = serialized.to_json_string().unwrap();
        assert!(json.contains("\"content_type\":\"application/octet-stream\""));

        let restored = serialized.to_event().unwrap();
        assert_eq!(restored.payload().data(), bytes.as_slice());
        assert_eq!(restored.payload().content_type(), ContentType::Binary);
    }

    #[test]
    fn json_payload_stays_human_readable() {
        let event = event_with_key(
            Payload::from_json(&serde_json::json!({"k": "v"})).unwrap(),
            "k",
        );
        let serialized = SerializedEvent::from_event(&event).unwrap();
        match &serialized.payload {
            SerializedPayload::Json(value) => {
                assert_eq!(value, &serde_json::json!({"k": "v"}));
            }
            other => panic!("expected Json, got {other:?}"),
        }
        let json = serialized.to_json_string().unwrap();
        assert!(json.contains("\"content_type\":\"application/json\""));
        assert!(json.contains("\"data\":{\"k\":\"v\"}"));
    }

    #[test]
    fn json_value_round_trip() {
        let event = Event::builder(
            "acme",
            "/task",
            "task.created",
            Payload::from_json(&serde_json::json!({"key": "value"})).unwrap(),
        )
        .unwrap()
        .key("task-123")
        .unwrap()
        .build()
        .unwrap();

        let serialized = SerializedEvent::from_event(&event).unwrap();
        let value = serialized.to_json_value();
        let parsed = SerializedEvent::from_json_value(value).unwrap();

        assert_eq!(parsed.id, serialized.id);
        assert_eq!(parsed.topic, serialized.topic);
        assert_eq!(parsed.payload.content_type(), ContentType::Json);
    }

    #[test]
    fn json_string_round_trip() {
        let event = Event::builder(
            "acme",
            "/task",
            "task.created",
            Payload::from_json(&serde_json::json!({"key": "value"})).unwrap(),
        )
        .unwrap()
        .key("task-123")
        .unwrap()
        .build()
        .unwrap();

        let serialized = SerializedEvent::from_event(&event).unwrap();
        let s = serialized.to_json_string().unwrap();
        let parsed = SerializedEvent::from_json_str(&s).unwrap();

        assert_eq!(parsed.id, serialized.id);
        assert_eq!(parsed.namespace, serialized.namespace);
        assert_eq!(parsed.organization, serialized.organization);
    }

    #[test]
    fn from_json_slice_roundtrip() {
        let event = Event::builder(
            "acme",
            "/task",
            "task.created",
            Payload::from_json(&serde_json::json!({"key": "value"})).unwrap(),
        )
        .unwrap()
        .key("task-123")
        .unwrap()
        .build()
        .unwrap();

        let serialized = SerializedEvent::from_event(&event).unwrap();
        let bytes = serialized.to_json_string().unwrap().into_bytes();
        let parsed = SerializedEvent::from_json_slice(&bytes).unwrap();

        assert_eq!(parsed.id, serialized.id);
        assert_eq!(parsed.topic, serialized.topic);
        assert_eq!(parsed.payload.content_type(), ContentType::Json);
    }

    #[test]
    fn json_string_round_trip_with_binary() {
        let bytes = vec![0xde, 0xad, 0xbe, 0xef];
        let event = event_with_key(Payload::from_bytes(bytes.clone()), "b1");

        let serialized = SerializedEvent::from_event(&event).unwrap();
        let s = serialized.to_json_string().unwrap();
        let parsed = SerializedEvent::from_json_str(&s).unwrap();
        let restored = parsed.to_event().unwrap();

        assert_eq!(restored.payload().data(), bytes.as_slice());
    }

    #[test]
    fn lineage_fields_roundtrip() {
        let parent_id = EventId::new();
        let event = Event::builder("acme", "/x", "thing.happened", Payload::from_string("p"))
            .unwrap()
            .key("k")
            .unwrap()
            .parent_id(parent_id)
            .correlation_id("corr")
            .unwrap()
            .causation_id("cause")
            .unwrap()
            .build()
            .unwrap();

        let serialized = SerializedEvent::from_event(&event).unwrap();
        assert_eq!(serialized.key.as_deref(), Some("k"));
        assert_eq!(
            serialized.parent_id.as_deref(),
            Some(parent_id.to_string().as_str())
        );
        assert_eq!(serialized.correlation_id.as_deref(), Some("corr"));
        assert_eq!(serialized.causation_id.as_deref(), Some("cause"));

        let restored = serialized.to_event().unwrap();
        assert_eq!(restored.key().map(EventKey::as_str), Some("k"));
        assert_eq!(restored.parent_id(), Some(parent_id));
        assert_eq!(
            restored.correlation_id().map(EventKey::as_str),
            Some("corr")
        );
        assert_eq!(restored.causation_id().map(EventKey::as_str), Some("cause"));
    }
}
