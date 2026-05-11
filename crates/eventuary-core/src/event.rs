use std::fmt;
use std::result::Result as StdResult;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::error::Result;
use crate::event_key::EventKey;
use crate::metadata::{CAUSATION_ID, CORRELATION_ID, Metadata};
use crate::namespace::Namespace;
use crate::organization::OrganizationId;
use crate::payload::Payload;
use crate::topic::Topic;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EventId(Uuid);

impl EventId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for EventId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for EventId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> StdResult<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

#[derive(Debug, Clone)]
pub struct Event {
    id: EventId,
    organization: OrganizationId,
    namespace: Namespace,
    topic: Topic,
    key: EventKey,
    payload: Payload,
    metadata: Metadata,
    timestamp: DateTime<Utc>,
    version: u64,
}

impl Event {
    pub fn create(
        organization: impl Into<String>,
        namespace: impl Into<String>,
        topic: impl Into<String>,
        key: impl Into<String>,
        payload: Payload,
    ) -> Result<Self> {
        Ok(Self {
            id: EventId::new(),
            organization: OrganizationId::new(organization)?,
            namespace: Namespace::new(namespace)?,
            topic: Topic::new(topic)?,
            key: EventKey::new(key)?,
            payload,
            metadata: Metadata::new(),
            timestamp: Utc::now(),
            version: 1,
        })
    }

    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn with_correlation_id(mut self, id: impl Into<String>) -> Result<Self> {
        self.metadata = self.metadata.with(CORRELATION_ID, id.into())?;
        Ok(self)
    }

    pub fn with_causation_id(mut self, id: impl Into<String>) -> Result<Self> {
        self.metadata = self.metadata.with(CAUSATION_ID, id.into())?;
        Ok(self)
    }

    pub fn correlation_id(&self) -> Option<&str> {
        self.metadata
            .as_map()
            .get(CORRELATION_ID)
            .map(String::as_str)
    }

    pub fn causation_id(&self) -> Option<&str> {
        self.metadata.as_map().get(CAUSATION_ID).map(String::as_str)
    }

    pub fn restore(r: RestoreEvent) -> Self {
        Self {
            id: r.id,
            organization: r.organization,
            namespace: r.namespace,
            topic: r.topic,
            key: r.key,
            payload: r.payload,
            metadata: r.metadata,
            timestamp: r.timestamp,
            version: r.version,
        }
    }

    pub fn id(&self) -> EventId {
        self.id
    }
    pub fn organization(&self) -> &OrganizationId {
        &self.organization
    }
    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }
    pub fn topic(&self) -> &Topic {
        &self.topic
    }
    pub fn key(&self) -> &EventKey {
        &self.key
    }
    pub fn payload(&self) -> &Payload {
        &self.payload
    }
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }
    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }
    pub fn version(&self) -> u64 {
        self.version
    }
}

pub struct RestoreEvent {
    pub id: EventId,
    pub organization: OrganizationId,
    pub namespace: Namespace,
    pub topic: Topic,
    pub key: EventKey,
    pub payload: Payload,
    pub metadata: Metadata,
    pub timestamp: DateTime<Utc>,
    pub version: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_event() {
        let payload = Payload::from_json(&serde_json::json!({"task_id": "123"})).unwrap();
        let event = Event::create("acme", "/task", "task.created", "task-123", payload).unwrap();
        assert_eq!(event.organization().as_str(), "acme");
        assert_eq!(event.namespace().as_str(), "/task");
        assert_eq!(event.topic().as_str(), "task.created");
        assert_eq!(event.key().as_str(), "task-123");
        assert_eq!(event.version(), 1);
    }

    #[test]
    fn create_with_metadata() {
        let payload = Payload::from_string("test");
        let metadata = Metadata::new()
            .with("agent_id", "abc-123")
            .unwrap()
            .with("project", "acme")
            .unwrap();
        let event = Event::create("acme", "/agent", "agent.registered", "agent-1", payload)
            .unwrap()
            .with_metadata(metadata);
        assert_eq!(event.metadata().get("agent_id"), Some("abc-123"));
        assert_eq!(event.metadata().get("project"), Some("acme"));
    }

    #[test]
    fn correlation_and_causation_ids_roundtrip_through_metadata() {
        let payload = Payload::from_string("test");
        let event = Event::create("acme", "/x", "thing.happened", "k", payload)
            .unwrap()
            .with_correlation_id("corr-1")
            .unwrap()
            .with_causation_id("cause-1")
            .unwrap();
        assert_eq!(event.correlation_id(), Some("corr-1"));
        assert_eq!(event.causation_id(), Some("cause-1"));
        assert_eq!(event.metadata().get(CORRELATION_ID), Some("corr-1"));
        assert_eq!(event.metadata().get(CAUSATION_ID), Some("cause-1"));
    }
}
