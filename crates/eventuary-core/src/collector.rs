use chrono::{DateTime, Utc};

use crate::domain_event::DomainEvent;
use crate::error::Result;
use crate::event::Event;
use crate::organization::OrganizationId;
use crate::payload::Payload;
use crate::version::{AggregateVersion, SchemaVersion};

#[derive(Debug, Clone)]
pub struct EventCollector<P = Payload> {
    organization: OrganizationId,
    version: Option<AggregateVersion>,
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
            version: None,
            pending: Vec::new(),
        }
    }

    pub fn restored(organization: OrganizationId, version: AggregateVersion) -> Self {
        Self {
            organization,
            version: Some(version),
            pending: Vec::new(),
        }
    }

    pub fn collect(&mut self, event: Event<P>) {
        self.pending.push(event);
    }

    #[must_use = "dropping the changes loses the events and the version they must be saved with"]
    pub fn take_changes(&mut self) -> Result<Option<Changes<P>>> {
        if self.pending.is_empty() {
            return Ok(None);
        }
        let expected = self.version;
        let version = match expected {
            Some(version) => version.next()?,
            None => AggregateVersion::FIRST,
        };
        self.version = Some(version);
        Ok(Some(Changes {
            expected,
            version,
            events: std::mem::take(&mut self.pending),
        }))
    }

    pub fn organization(&self) -> &OrganizationId {
        &self.organization
    }

    pub fn version(&self) -> Option<AggregateVersion> {
        self.version
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

    #[test]
    fn a_collector_without_pending_events_has_no_changes() {
        let mut events = EventCollector::<Payload>::restored(
            OrganizationId::platform(),
            AggregateVersion::FIRST,
        );

        assert!(events.take_changes().unwrap().is_none());
        assert_eq!(events.version(), Some(AggregateVersion::FIRST));
    }

    #[test]
    fn the_first_changes_create_version_one() {
        let mut events = collector();
        events.record(placed(), at()).unwrap();

        let changes = events.take_changes().unwrap().unwrap();

        assert!(changes.is_creation());
        assert_eq!(changes.expected(), None);
        assert_eq!(changes.version(), AggregateVersion::FIRST);
        assert_eq!(changes.events().len(), 1);
        assert!(!events.has_pending());
        assert_eq!(events.version(), Some(AggregateVersion::FIRST));
    }

    #[test]
    fn later_changes_expect_the_loaded_version_and_advance_it_once() {
        let loaded = AggregateVersion::new(4).unwrap();
        let mut events = EventCollector::restored(OrganizationId::new("acme").unwrap(), loaded);
        events.record(placed(), at()).unwrap();
        events.record(placed(), at()).unwrap();

        let changes = events.take_changes().unwrap().unwrap();

        assert!(!changes.is_creation());
        assert_eq!(changes.expected(), Some(loaded));
        assert_eq!(changes.version().value(), 5);
        assert_eq!(changes.into_events().len(), 2);
        assert_eq!(events.version().unwrap().value(), 5);
    }

    #[test]
    fn collect_accepts_an_event_built_by_hand() {
        let mut events = collector();
        let event = Event::create(
            "acme",
            "/x",
            "thing.happened",
            "k",
            Payload::from_string("p"),
        )
        .unwrap();

        events.collect(event);

        assert_eq!(events.pending().len(), 1);
    }
}
