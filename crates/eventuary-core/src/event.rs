use std::fmt;
use std::result::Result as StdResult;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use std::num::NonZeroU16;

use crate::error::Result;
use crate::event_key::{EventKey, Partition, fnv1a_u64};
use crate::metadata::Metadata;
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
pub struct Event<P = Payload> {
    id: EventId,
    organization: OrganizationId,
    namespace: Namespace,
    topic: Topic,
    payload: P,
    metadata: Metadata,
    timestamp: DateTime<Utc>,
    version: u64,

    key: Option<EventKey>,
    parent_id: Option<EventId>,
    correlation_id: Option<EventKey>,
    causation_id: Option<EventKey>,
}

pub struct EventBuilder<P = Payload> {
    organization: OrganizationId,
    namespace: Namespace,
    topic: Topic,
    payload: P,
    metadata: Metadata,
    key: Option<EventKey>,
    parent_id: Option<EventId>,
    correlation_id: Option<EventKey>,
    causation_id: Option<EventKey>,
}

impl<P> EventBuilder<P> {
    fn new(organization: OrganizationId, namespace: Namespace, topic: Topic, payload: P) -> Self {
        Self {
            organization,
            namespace,
            topic,
            payload,
            metadata: Metadata::new(),
            key: None,
            parent_id: None,
            correlation_id: None,
            causation_id: None,
        }
    }

    pub fn key(mut self, key: impl Into<String>) -> Result<Self> {
        self.key = Some(EventKey::new(key)?);
        Ok(self)
    }

    pub fn parent_id(mut self, parent_id: EventId) -> Self {
        self.parent_id = Some(parent_id);
        self
    }

    pub fn correlation_id(mut self, correlation_id: impl Into<String>) -> Result<Self> {
        self.correlation_id = Some(EventKey::new(correlation_id)?);
        Ok(self)
    }

    pub fn causation_id(mut self, causation_id: impl Into<String>) -> Result<Self> {
        self.causation_id = Some(EventKey::new(causation_id)?);
        Ok(self)
    }

    pub fn metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn build(self) -> Result<Event<P>> {
        Event::new(
            EventId::new(),
            self.organization,
            self.namespace,
            self.topic,
            self.payload,
            self.metadata,
            Utc::now(),
            1,
            self.key,
            self.parent_id,
            self.correlation_id,
            self.causation_id,
        )
    }
}

impl<P> Event<P> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: EventId,
        organization: OrganizationId,
        namespace: Namespace,
        topic: Topic,
        payload: P,
        metadata: Metadata,
        timestamp: DateTime<Utc>,
        version: u64,
        key: Option<EventKey>,
        parent_id: Option<EventId>,
        correlation_id: Option<EventKey>,
        causation_id: Option<EventKey>,
    ) -> Result<Self> {
        Ok(Self {
            id,
            organization,
            namespace,
            topic,
            payload,
            metadata,
            timestamp,
            version,
            key,
            parent_id,
            correlation_id,
            causation_id,
        })
    }

    pub fn builder(
        organization: impl Into<String>,
        namespace: impl Into<String>,
        topic: impl Into<String>,
        payload: P,
    ) -> Result<EventBuilder<P>> {
        Ok(EventBuilder::new(
            OrganizationId::new(organization)?,
            Namespace::new(namespace)?,
            Topic::new(topic)?,
            payload,
        ))
    }

    pub fn create(
        organization: impl Into<String>,
        namespace: impl Into<String>,
        topic: impl Into<String>,
        payload: P,
    ) -> Result<Self> {
        Self::builder(organization, namespace, topic, payload)?.build()
    }

    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
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
    pub fn payload(&self) -> &P {
        &self.payload
    }
    pub fn into_payload(self) -> P {
        self.payload
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
    pub fn key(&self) -> Option<&EventKey> {
        self.key.as_ref()
    }

    /// Determine the partition for this event within a given count.
    /// Uses the event key if present, falls back to event id bytes.
    #[deprecated(
        since = "0.1.0-alpha.2",
        note = "Use the resolver/hasher pipeline via `PartitionHasher::partition_for(&PartitionKey, count)`. \
                See `PartitionRouteStrategy::ResolverHasher` for `PartitionedReader` integration."
    )]
    #[allow(deprecated)]
    pub fn partition(&self, count: NonZeroU16) -> Partition {
        match self.key() {
            Some(key) => key.partition_for(count),
            None => {
                let id = (fnv1a_u64(self.id().as_uuid().as_bytes()) % count.get() as u64) as u16;
                Partition::new(id, count).expect("id < count by modulo")
            }
        }
    }
    pub fn parent_id(&self) -> Option<EventId> {
        self.parent_id
    }
    pub fn correlation_id(&self) -> Option<&EventKey> {
        self.correlation_id.as_ref()
    }
    pub fn causation_id(&self) -> Option<&EventKey> {
        self.causation_id.as_ref()
    }

    pub fn map_payload<Q, F>(self, f: F) -> Event<Q>
    where
        F: FnOnce(P) -> Q,
    {
        Event {
            id: self.id,
            organization: self.organization,
            namespace: self.namespace,
            topic: self.topic,
            payload: f(self.payload),
            metadata: self.metadata,
            timestamp: self.timestamp,
            version: self.version,
            key: self.key,
            parent_id: self.parent_id,
            correlation_id: self.correlation_id,
            causation_id: self.causation_id,
        }
    }

    pub fn try_map_payload<Q, F>(self, f: F) -> Result<Event<Q>>
    where
        F: FnOnce(P) -> Result<Q>,
    {
        Ok(Event {
            id: self.id,
            organization: self.organization,
            namespace: self.namespace,
            topic: self.topic,
            payload: f(self.payload)?,
            metadata: self.metadata,
            timestamp: self.timestamp,
            version: self.version,
            key: self.key,
            parent_id: self.parent_id,
            correlation_id: self.correlation_id,
            causation_id: self.causation_id,
        })
    }

    pub fn encode_payload<C>(&self, codec: &C) -> Result<Event<Payload>>
    where
        C: crate::PayloadCodec<P>,
    {
        Event::new(
            self.id,
            self.organization.clone(),
            self.namespace.clone(),
            self.topic.clone(),
            codec.encode(&self.payload)?,
            self.metadata.clone(),
            self.timestamp,
            self.version,
            self.key.clone(),
            self.parent_id,
            self.correlation_id.clone(),
            self.causation_id.clone(),
        )
    }
}

impl Event<Payload> {
    pub fn decode_payload<P, C>(self, codec: &C) -> Result<Event<P>>
    where
        C: crate::PayloadCodec<P>,
    {
        self.try_map_payload(|payload| codec.decode(&payload))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_event_without_optional_key() {
        let payload = Payload::from_json(&serde_json::json!({"task_id": "123"})).unwrap();
        let event = Event::create("acme", "/task", "task.created", payload).unwrap();
        assert_eq!(event.organization().as_str(), "acme");
        assert_eq!(event.namespace().as_str(), "/task");
        assert_eq!(event.topic().as_str(), "task.created");
        assert_eq!(event.key(), None);
        assert_eq!(event.parent_id(), None);
        assert_eq!(event.correlation_id(), None);
        assert_eq!(event.causation_id(), None);
        assert_eq!(event.version(), 1);
    }

    #[test]
    fn builder_sets_optional_lineage_fields() {
        let parent_id = EventId::new();
        let event = Event::builder("acme", "/x", "thing.happened", Payload::from_string("p"))
            .unwrap()
            .key("entity-1")
            .unwrap()
            .parent_id(parent_id)
            .correlation_id("workflow-7")
            .unwrap()
            .causation_id("command-9")
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(event.key().map(EventKey::as_str), Some("entity-1"));
        assert_eq!(event.parent_id(), Some(parent_id));
        assert_eq!(
            event.correlation_id().map(EventKey::as_str),
            Some("workflow-7")
        );
        assert_eq!(
            event.causation_id().map(EventKey::as_str),
            Some("command-9")
        );
    }

    #[test]
    fn builder_rejects_empty_optional_ids() {
        let builder =
            Event::builder("acme", "/x", "thing.happened", Payload::from_string("p")).unwrap();
        assert!(builder.key("").is_err());

        let builder =
            Event::builder("acme", "/x", "thing.happened", Payload::from_string("p")).unwrap();
        assert!(builder.correlation_id("").is_err());

        let builder =
            Event::builder("acme", "/x", "thing.happened", Payload::from_string("p")).unwrap();
        assert!(builder.causation_id("").is_err());
    }

    #[test]
    fn create_with_metadata() {
        let payload = Payload::from_string("test");
        let metadata = Metadata::new()
            .with("agent_id", "abc-123")
            .unwrap()
            .with("project", "acme")
            .unwrap();
        let event = Event::builder("acme", "/agent", "agent.registered", payload)
            .unwrap()
            .metadata(metadata)
            .build()
            .unwrap();
        assert_eq!(event.metadata().get("agent_id"), Some("abc-123"));
        assert_eq!(event.metadata().get("project"), Some("acme"));
    }

    #[test]
    fn correlation_and_causation_are_first_class_fields() {
        let event = Event::builder("acme", "/x", "thing.happened", Payload::from_string("test"))
            .unwrap()
            .correlation_id("corr-1")
            .unwrap()
            .causation_id("cause-1")
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(event.correlation_id().map(EventKey::as_str), Some("corr-1"));
        assert_eq!(event.causation_id().map(EventKey::as_str), Some("cause-1"));
        assert!(event.metadata().is_empty());
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct UserUpdated {
        user_id: String,
        email: String,
    }

    #[test]
    fn create_event_with_typed_payload() {
        let payload = UserUpdated {
            user_id: "u-1".to_owned(),
            email: "a@example.com".to_owned(),
        };

        let event: Event<UserUpdated> =
            Event::create("acme", "/users", "user.updated", payload).unwrap();

        assert_eq!(event.payload().user_id, "u-1");
        assert_eq!(event.organization().as_str(), "acme");
        assert_eq!(event.namespace().as_str(), "/users");
        assert_eq!(event.topic().as_str(), "user.updated");
    }

    #[test]
    fn map_payload_changes_only_payload_type() {
        let event: Event<UserUpdated> = Event::create(
            "acme",
            "/users",
            "user.updated",
            UserUpdated {
                user_id: "u-1".to_owned(),
                email: "a@example.com".to_owned(),
            },
        )
        .unwrap()
        .with_metadata(Metadata::new().with("source", "test").unwrap());

        let id = event.id();
        let mapped: Event<String> = event.map_payload(|payload| payload.email);

        assert_eq!(mapped.id(), id);
        assert_eq!(mapped.payload(), "a@example.com");
        assert_eq!(mapped.metadata().get("source"), Some("test"));
    }

    #[test]
    #[allow(deprecated)]
    fn event_partition_works_for_typed_payloads() {
        use std::num::NonZeroU16;

        let count = NonZeroU16::new(16).unwrap();
        let event: Event<UserUpdated> = Event::builder(
            "acme",
            "/users",
            "user.updated",
            UserUpdated {
                user_id: "u-1".to_owned(),
                email: "a@example.com".to_owned(),
            },
        )
        .unwrap()
        .key("user-u-1")
        .unwrap()
        .build()
        .unwrap();

        assert_eq!(
            event.partition(count),
            EventKey::new("user-u-1").unwrap().partition_for(count)
        );
    }

    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    struct SerializableUserUpdated {
        user_id: String,
        email: String,
    }

    #[test]
    fn event_encodes_and_decodes_payload_with_codec() {
        use crate::JsonPayloadCodec;

        let typed: Event<SerializableUserUpdated> = Event::create(
            "acme",
            "/users",
            "user.updated",
            SerializableUserUpdated {
                user_id: "u-1".to_owned(),
                email: "a@example.com".to_owned(),
            },
        )
        .unwrap();

        let id = typed.id();
        let encoded = typed.encode_payload(&JsonPayloadCodec).unwrap();
        assert_eq!(encoded.id(), id);
        assert_eq!(encoded.payload().content_type(), crate::ContentType::Json);

        let decoded: Event<SerializableUserUpdated> =
            encoded.decode_payload(&JsonPayloadCodec).unwrap();
        assert_eq!(decoded.id(), id);
        assert_eq!(decoded.payload().user_id, "u-1");
    }
}
