use std::collections::HashMap;
use std::str::FromStr;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedEvent {
    pub id: String,
    pub organization: String,
    pub namespace: String,
    pub topic: String,
    #[serde(default)]
    pub key: Option<String>,
    pub payload: serde_json::Value,
    pub content_type: String,
    pub metadata: HashMap<String, String>,
    pub timestamp: DateTime<Utc>,
    pub version: u64,
    #[serde(default)]
    pub parent_id: Option<String>,
    #[serde(default)]
    pub correlation_id: Option<String>,
    #[serde(default)]
    pub causation_id: Option<String>,
}

impl SerializedEvent {
    pub fn from_event(event: &Event) -> Result<Self> {
        let payload_value = match event.payload().content_type() {
            ContentType::Json => serde_json::from_slice(event.payload().data())
                .map_err(|e| Error::Serialization(e.to_string()))?,
            ContentType::PlainText => {
                let text = std::str::from_utf8(event.payload().data())
                    .map_err(|e| Error::Serialization(e.to_string()))?;
                serde_json::Value::String(text.to_owned())
            }
            ContentType::Binary => {
                let encoded = BASE64.encode(event.payload().data());
                serde_json::Value::String(encoded)
            }
        };

        Ok(Self {
            id: event.id().to_string(),
            organization: event.organization().to_string(),
            namespace: event.namespace().to_string(),
            topic: event.topic().to_string(),
            key: event.key().map(|key| key.to_string()),
            payload: payload_value,
            content_type: event.payload().content_type().to_string(),
            metadata: event.metadata().as_map().clone(),
            timestamp: event.timestamp(),
            version: event.version(),
            parent_id: event.parent_id().map(|id| id.to_string()),
            correlation_id: event.correlation_id().map(|id| id.to_string()),
            causation_id: event.causation_id().map(|id| id.to_string()),
        })
    }

    pub fn to_event(&self) -> Result<Event> {
        let content_type: ContentType = self
            .content_type
            .parse()
            .map_err(|e: Error| Error::Serialization(e.to_string()))?;

        let data = match content_type {
            ContentType::Json => serde_json::to_vec(&self.payload)
                .map_err(|e| Error::Serialization(e.to_string()))?,
            ContentType::PlainText => {
                let text = self.payload.as_str().ok_or_else(|| {
                    Error::Serialization("plain text payload must be a JSON string".to_owned())
                })?;
                text.as_bytes().to_vec()
            }
            ContentType::Binary => {
                let encoded = self.payload.as_str().ok_or_else(|| {
                    Error::Serialization("binary payload must be a base64 JSON string".to_owned())
                })?;
                BASE64
                    .decode(encoded)
                    .map_err(|e| Error::Serialization(e.to_string()))?
            }
        };

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
            Payload::from_raw(data, content_type),
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
    fn plain_text_roundtrip_preserves_string() {
        let event = event_with_key(Payload::from_string("hello world"), "k");

        let serialized = SerializedEvent::from_event(&event).unwrap();
        assert_eq!(
            serialized.payload,
            serde_json::Value::String("hello world".to_owned())
        );

        let restored = serialized.to_event().unwrap();
        assert_eq!(restored.payload().data(), b"hello world");
        assert_eq!(restored.payload().content_type(), ContentType::PlainText);
    }

    #[test]
    fn binary_payload_base64_round_trip_preserves_bytes() {
        let bytes = vec![0xff, 0x00, 0x01, 0xfe, 0x80, 0x7f, 0x10];
        let event = event_with_key(Payload::from_bytes(bytes.clone()), "k");

        let serialized = SerializedEvent::from_event(&event).unwrap();
        assert!(serialized.payload.is_string());

        let restored = serialized.to_event().unwrap();
        assert_eq!(restored.payload().data(), bytes.as_slice());
        assert_eq!(restored.payload().content_type(), ContentType::Binary);
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
        assert_eq!(parsed.payload, serialized.payload);
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
        assert_eq!(parsed.payload, serialized.payload);
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
