use std::collections::HashMap;
use std::str::FromStr;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::event::{Event, EventId, RestoreEvent};
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
    pub key: String,
    pub payload: serde_json::Value,
    pub content_type: String,
    pub metadata: HashMap<String, String>,
    pub timestamp: DateTime<Utc>,
    pub version: u64,
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
            key: event.key().to_string(),
            payload: payload_value,
            content_type: event.payload().content_type().to_string(),
            metadata: event.metadata().as_map().clone(),
            timestamp: event.timestamp(),
            version: event.version(),
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

        Ok(Event::restore(RestoreEvent {
            id: EventId::from_str(&self.id).map_err(|e| Error::Serialization(e.to_string()))?,
            organization: OrganizationId::new(&self.organization)?,
            namespace: Namespace::new(&self.namespace)?,
            topic: Topic::new(&self.topic)?,
            key: EventKey::new(&self.key)?,
            payload: Payload::from_raw(data, content_type),
            metadata: Metadata::from(self.metadata.clone()),
            timestamp: self.timestamp,
            version: self.version,
        }))
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let payload = Payload::from_json(&serde_json::json!({"key": "value"})).unwrap();
        let event = Event::create("acme", "/task", "task.created", "task-123", payload).unwrap();

        let serialized = SerializedEvent::from_event(&event).unwrap();
        assert_eq!(serialized.topic, "task.created");
        assert_eq!(serialized.namespace, "/task");
        assert_eq!(serialized.organization, "acme");
        assert_eq!(serialized.key, "task-123");

        let restored = serialized.to_event().unwrap();
        assert_eq!(restored.topic().as_str(), "task.created");
        assert_eq!(restored.id(), event.id());
    }

    #[test]
    fn plain_text_roundtrip_preserves_string() {
        let payload = Payload::from_string("hello world");
        let event = Event::create("acme", "/x", "thing.happened", "k", payload).unwrap();

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
        let payload = Payload::from_bytes(bytes.clone());
        let event = Event::create("acme", "/x", "thing.happened", "k", payload).unwrap();

        let serialized = SerializedEvent::from_event(&event).unwrap();
        assert!(serialized.payload.is_string());

        let restored = serialized.to_event().unwrap();
        assert_eq!(restored.payload().data(), bytes.as_slice());
        assert_eq!(restored.payload().content_type(), ContentType::Binary);
    }

    #[test]
    fn json_value_round_trip() {
        let payload = Payload::from_json(&serde_json::json!({"key": "value"})).unwrap();
        let event = Event::create("acme", "/task", "task.created", "task-123", payload).unwrap();

        let serialized = SerializedEvent::from_event(&event).unwrap();
        let value = serialized.to_json_value();
        let parsed = SerializedEvent::from_json_value(value).unwrap();

        assert_eq!(parsed.id, serialized.id);
        assert_eq!(parsed.topic, serialized.topic);
        assert_eq!(parsed.payload, serialized.payload);
    }

    #[test]
    fn json_string_round_trip() {
        let payload = Payload::from_json(&serde_json::json!({"key": "value"})).unwrap();
        let event = Event::create("acme", "/task", "task.created", "task-123", payload).unwrap();

        let serialized = SerializedEvent::from_event(&event).unwrap();
        let s = serialized.to_json_string().unwrap();
        let parsed = SerializedEvent::from_json_str(&s).unwrap();

        assert_eq!(parsed.id, serialized.id);
        assert_eq!(parsed.namespace, serialized.namespace);
        assert_eq!(parsed.organization, serialized.organization);
    }

    #[test]
    fn json_string_round_trip_with_binary() {
        let bytes = vec![0xde, 0xad, 0xbe, 0xef];
        let payload = Payload::from_bytes(bytes.clone());
        let event = Event::create("acme", "/x", "blob.received", "b1", payload).unwrap();

        let serialized = SerializedEvent::from_event(&event).unwrap();
        let s = serialized.to_json_string().unwrap();
        let parsed = SerializedEvent::from_json_str(&s).unwrap();
        let restored = parsed.to_event().unwrap();

        assert_eq!(restored.payload().data(), bytes.as_slice());
    }
}
