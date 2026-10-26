use chrono::{DateTime, Utc};

use crate::domain_event::DomainEvent;
use crate::error::Result;
use crate::event::Event;
use crate::metadata::Metadata;
use crate::organization::OrganizationId;
use crate::payload::Payload;
use crate::version::{AggregateVersion, SchemaVersion};

pub const AGGREGATE_VERSION_KEY: &str = "aggregate_version";

#[derive(Debug, Clone)]
pub struct EventCollector<P = Payload> {
    organization: OrganizationId,
    stored: Option<AggregateVersion>,
    next: AggregateVersion,
    pending: Vec<Event<P>>,
}

#[derive(Debug, Clone)]
#[must_use = "dropping the changes loses the events and the version they must be saved with"]
pub struct Changes<P = Payload> {
    expected: Option<AggregateVersion>,
    version: AggregateVersion,
    events: Vec<Event<P>>,
}

impl<P> EventCollector<P> {
    pub fn new(organization: OrganizationId) -> Self {
        Self {
            organization,
            stored: None,
            next: AggregateVersion::FIRST,
            pending: Vec::new(),
        }
    }

    pub fn restored(organization: OrganizationId, version: AggregateVersion) -> Result<Self> {
        Ok(Self {
            organization,
            stored: Some(version),
            next: version.next()?,
            pending: Vec::new(),
        })
    }

    pub fn collect(&mut self, event: Event<P>) -> Result<()> {
        let metadata = event
            .metadata()
            .clone()
            .with(AGGREGATE_VERSION_KEY, self.next.to_string())?;
        self.pending.push(event.with_metadata(metadata));
        Ok(())
    }

    #[must_use = "dropping the changes loses the events and the version they must be saved with"]
    pub fn take_changes(&mut self) -> Option<Changes<P>> {
        if self.pending.is_empty() {
            return None;
        }
        let expected = self.stored.replace(self.next);
        Some(Changes {
            expected,
            version: self.next,
            events: std::mem::take(&mut self.pending),
        })
    }

    pub fn organization(&self) -> &OrganizationId {
        &self.organization
    }

    pub fn stored_version(&self) -> Option<AggregateVersion> {
        self.stored
    }

    pub fn next_version(&self) -> AggregateVersion {
        self.next
    }

    pub fn pending(&self) -> &[Event<P>] {
        &self.pending
    }

    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }
}

impl EventCollector<Payload> {
    pub fn record<E: DomainEvent>(&mut self, event: E, timestamp: DateTime<Utc>) -> Result<()> {
        let recorded = Event::builder(
            self.organization.as_str(),
            E::NAMESPACE,
            E::TOPIC,
            event.key(),
            Payload::from_json(&event)?,
        )?
        .schema_version(SchemaVersion::new(E::SCHEMA_VERSION)?)
        .metadata(Metadata::new().with(AGGREGATE_VERSION_KEY, self.next.to_string())?)
        .timestamp(timestamp)
        .build()?;
        self.pending.push(recorded);
        Ok(())
    }
}

impl<P> Changes<P> {
    pub fn expected(&self) -> Option<AggregateVersion> {
        self.expected
    }

    pub fn version(&self) -> AggregateVersion {
        self.version
    }

    pub fn is_creation(&self) -> bool {
        self.expected.is_none()
    }

    pub fn events(&self) -> &[Event<P>] {
        &self.events
    }

    pub fn into_events(self) -> Vec<Event<P>> {
        self.events
    }
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use super::*;

    #[derive(Debug, Serialize)]
    struct OrderPlaced {
        order_id: String,
        total: u64,
    }

    impl DomainEvent for OrderPlaced {
        const NAMESPACE: &'static str = "/orders";
        const TOPIC: &'static str = "order.placed";
        const SCHEMA_VERSION: u32 = 2;

        fn key(&self) -> String {
            self.order_id.clone()
        }
    }

    #[derive(Debug, Serialize)]
    struct OrderPlacedWithoutTopic {
        order_id: String,
    }

    impl DomainEvent for OrderPlacedWithoutTopic {
        const NAMESPACE: &'static str = "/orders";
        const TOPIC: &'static str = "";

        fn key(&self) -> String {
            self.order_id.clone()
        }
    }

    fn placed() -> OrderPlaced {
        OrderPlaced {
            order_id: "order-1".to_owned(),
            total: 42,
        }
    }

    fn at() -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000, 0).unwrap()
    }

    fn collector() -> EventCollector {
        EventCollector::new(OrganizationId::new("acme").unwrap())
    }

    #[test]
    fn record_builds_the_event_from_the_typed_fact() {
        let mut events = collector();

        events.record(placed(), at()).unwrap();

        let event = &events.pending()[0];
        assert_eq!(event.organization().as_str(), "acme");
        assert_eq!(event.namespace().as_str(), "/orders");
        assert_eq!(event.topic().as_str(), "order.placed");
        assert_eq!(event.key().as_str(), "order-1");
        assert_eq!(event.timestamp(), at());
        assert_eq!(event.schema_version(), SchemaVersion::new(2).unwrap());
        assert_eq!(
            event.payload().to_json::<serde_json::Value>().unwrap(),
            serde_json::json!({"order_id": "order-1", "total": 42})
        );
    }

    #[test]
    fn a_failed_record_leaves_the_collector_unchanged() {
        let mut events = collector();

        let result = events.record(
            OrderPlacedWithoutTopic {
                order_id: "order-1".to_owned(),
            },
            at(),
        );

        assert!(result.is_err());
        assert!(!events.has_pending());
    }

    fn loaded_at(version: u64) -> EventCollector {
        EventCollector::restored(
            OrganizationId::new("acme").unwrap(),
            AggregateVersion::new(version).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn a_collector_without_pending_events_has_no_changes() {
        let mut events = loaded_at(1);

        assert!(events.take_changes().is_none());
        assert_eq!(events.stored_version(), Some(AggregateVersion::FIRST));
    }

    #[test]
    fn the_first_changes_create_version_one() {
        let mut events = collector();
        events.record(placed(), at()).unwrap();

        let changes = events.take_changes().unwrap();

        assert!(changes.is_creation());
        assert_eq!(changes.expected(), None);
        assert_eq!(changes.version(), AggregateVersion::FIRST);
        assert_eq!(changes.events().len(), 1);
        assert!(!events.has_pending());
        assert_eq!(events.stored_version(), Some(AggregateVersion::FIRST));
    }

    #[test]
    fn changes_expect_the_loaded_version_and_store_the_next_one() {
        let mut events = loaded_at(4);
        events.record(placed(), at()).unwrap();
        events.record(placed(), at()).unwrap();

        let changes = events.take_changes().unwrap();

        assert!(!changes.is_creation());
        assert_eq!(changes.expected().unwrap().value(), 4);
        assert_eq!(changes.version().value(), 5);
        assert_eq!(changes.into_events().len(), 2);
        assert_eq!(events.stored_version().unwrap().value(), 5);
    }

    #[test]
    fn a_load_advances_the_version_once_however_often_it_is_saved() {
        let mut events = loaded_at(4);
        events.record(placed(), at()).unwrap();
        let first = events.take_changes().unwrap();
        events.record(placed(), at()).unwrap();

        let second = events.take_changes().unwrap();

        assert_eq!(first.expected().unwrap().value(), 4);
        assert_eq!(first.version().value(), 5);
        assert_eq!(second.expected().unwrap().value(), 5);
        assert_eq!(second.version().value(), 5);
        assert_eq!(events.next_version().value(), 5);
    }

    #[test]
    fn a_new_aggregate_saved_twice_creates_then_updates_version_one() {
        let mut events = collector();
        events.record(placed(), at()).unwrap();
        let first = events.take_changes().unwrap();
        events.record(placed(), at()).unwrap();

        let second = events.take_changes().unwrap();

        assert!(first.is_creation());
        assert!(!second.is_creation());
        assert_eq!(second.expected(), Some(AggregateVersion::FIRST));
        assert_eq!(second.version(), AggregateVersion::FIRST);
    }

    #[test]
    fn every_event_of_a_load_carries_the_version_it_is_saved_with() {
        let mut events = loaded_at(4);
        events.record(placed(), at()).unwrap();
        events.record(placed(), at()).unwrap();

        let changes = events.take_changes().unwrap();

        for event in changes.events() {
            assert_eq!(
                AggregateVersion::of(event).unwrap(),
                Some(changes.version())
            );
        }
    }

    #[test]
    fn restoring_the_last_possible_version_fails() {
        let result = EventCollector::<Payload>::restored(
            OrganizationId::platform(),
            AggregateVersion::new(u64::MAX).unwrap(),
        );

        assert!(result.is_err());
    }

    #[test]
    fn collect_accepts_an_event_built_by_hand() {
        let mut events = collector();
        let event = Event::builder(
            "acme",
            "/x",
            "thing.happened",
            "k",
            Payload::from_string("p"),
        )
        .unwrap()
        .metadata(Metadata::new().with("actor_id", "user-1").unwrap())
        .build()
        .unwrap();

        events.collect(event).unwrap();

        let collected = &events.pending()[0];
        assert_eq!(collected.metadata().get("actor_id"), Some("user-1"));
        assert_eq!(
            AggregateVersion::of(collected).unwrap(),
            Some(AggregateVersion::FIRST)
        );
    }
}
