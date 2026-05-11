use chrono::{DateTime, Utc};

use crate::{
    ConsumerGroupId, Event, EventKey, Filter, Metadata, Namespace, OrganizationId, StartFrom, Topic,
};

/// Read-side subscription: tells a [`Reader`] *which events* to deliver and
/// *how* to identify the consumer.
///
/// Subscriptions are passed to [`Reader::read`]; the reader uses them as
/// (a) backend hints for query construction (sqlite/postgres push topic and
/// namespace filters into SQL; kafka/sqs apply them in memory via
/// [`matches`]) and (b) per-event filter via [`matches`] before each event
/// is yielded.
///
/// # Filter semantics
///
/// [`matches`] checks each non-`None` field as a conjunction (logical AND).
/// A `None` field disables that predicate:
///
/// | Field | Predicate when `Some(_)` |
/// |-------|--------------------------|
/// | `organization` | event.organization == this (always required) |
/// | `topics` | event.topic is in the list (OR within list, AND with rest) |
/// | `namespace_prefix` | event.namespace starts with prefix |
/// | `keys` | event.key is in the list |
/// | `metadata` | every (k, v) pair in this is present in event.metadata (subset match, AND across pairs — no OR semantics) |
/// | `end_at` | event.timestamp <= end_at |
///
/// `start_from` and `limit` are not predicates: they are positional
/// (cursor-like) and count-like respectively, and are honored by the
/// backend's read loop rather than by [`matches`].
///
/// # Subscription vs. `Filter`
///
/// Subscriptions are applied **at read time**, at or near the backend, and
/// can prune work before bytes hit the consumer task. Use them to scope
/// *what is delivered*.
///
/// `EventSubscription` also implements [`io::Filter`], so the same predicate
/// can be reused in a consumer loop when filtering cannot be pushed down to
/// a backend.
///
/// As a rule of thumb: push everything you can into the subscription, and
/// use [`io::Filter`] for handler-specific concerns that depend on
/// downstream state.
///
/// # Backend constraints
///
/// Each backend may reject subscriptions that contradict its bound
/// construction-time configuration. For example, the Kafka reader binds
/// the consumer group and starting offset behavior at construction and
/// rejects subscriptions that try to override `start_from` or
/// `consumer_group_id`. The SQS reader only honors `StartFrom::Latest`
/// because queue semantics cannot seek. Backends document their
/// constraints on their `Reader::read` impl.
///
/// [`Reader`]: crate::io::Reader
/// [`Reader::read`]: crate::io::Reader::read
/// [`matches`]: EventSubscription::matches
/// [`io::Filter`]: crate::io::Filter
/// [`Handler`]: crate::io::Handler
#[derive(Debug, Clone)]
pub struct EventSubscription {
    /// Human-readable label for the subscription. Currently informational;
    /// some backends (sqlite, postgres) may also use it as a stream name
    /// for consumer-offset bookkeeping.
    pub name: Option<String>,
    /// Consumer-group identity for backends that support shared
    /// checkpointing (kafka, sqlite, postgres). `None` means a single
    /// reader with no persistent offset.
    pub consumer_group_id: Option<ConsumerGroupId>,

    /// Tenant scope. Always required. Events from other organizations are
    /// filtered out.
    pub organization: OrganizationId,
    /// Topic allow-list. `None` accepts any topic. `Some(vec![..])` accepts
    /// any topic in the list (OR within list).
    pub topics: Option<Vec<Topic>>,
    /// Namespace prefix filter. An event matches when its namespace starts
    /// with this prefix (`/billing` matches `/billing` and
    /// `/billing/invoices`).
    pub namespace_prefix: Option<Namespace>,
    /// Event-key allow-list. `None` accepts any key.
    pub keys: Option<Vec<EventKey>>,
    /// Metadata predicate: every key/value pair in this map must be present
    /// (with equal value) in the event's metadata. AND across pairs — no
    /// OR semantics. Use a separate subscription for alternatives.
    pub metadata: Option<Metadata>,
    /// Starting position. Honored by backends that can seek (sqlite,
    /// postgres, kafka at construction). Backends that cannot seek (sqs)
    /// require `StartFrom::Latest`.
    pub start_from: StartFrom,
    /// Optional upper-bound timestamp. Events with `timestamp > end_at`
    /// are filtered out by [`matches`]. Backends do not seek to it; they
    /// merely stop matching.
    pub end_at: Option<DateTime<Utc>>,
    /// Optional cap on the number of delivered events. The backend stops
    /// emitting once `limit` events have been yielded. Applied **after**
    /// filtering — counts only matched events.
    pub limit: Option<usize>,
}

impl EventSubscription {
    pub fn new(organization: OrganizationId) -> Self {
        Self {
            name: None,
            consumer_group_id: None,

            organization,
            topics: None,
            namespace_prefix: None,
            keys: None,
            metadata: None,
            start_from: StartFrom::Latest,
            end_at: None,
            limit: None,
        }
    }

    pub fn matches(&self, event: &Event) -> bool {
        if event.organization() != &self.organization {
            return false;
        }
        if let Some(topics) = self.topics.as_ref()
            && !topics.iter().any(|t| t == event.topic())
        {
            return false;
        }
        if let Some(prefix) = self.namespace_prefix.as_ref()
            && !event.namespace().starts_with(prefix)
        {
            return false;
        }
        if let Some(keys) = self.keys.as_ref()
            && !event
                .key()
                .is_some_and(|event_key| keys.iter().any(|key| key == event_key))
        {
            return false;
        }
        if let Some(metadata) = self.metadata.as_ref()
            && !metadata
                .as_map()
                .iter()
                .all(|(key, value)| event.metadata().get(key) == Some(value.as_str()))
        {
            return false;
        }
        if let Some(end_at) = self.end_at
            && event.timestamp() > end_at
        {
            return false;
        }
        true
    }
}

impl Filter for EventSubscription {
    fn matches(&self, event: &Event) -> bool {
        EventSubscription::matches(self, event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::Duration;

    use crate::Payload;

    fn ev(org: &str, topic: &str, namespace: &str) -> Event {
        ev_with_key(org, topic, namespace, "k")
    }

    fn ev_with_key(org: &str, topic: &str, namespace: &str, key: &str) -> Event {
        Event::builder(org, namespace, topic, Payload::from_string("p"))
            .unwrap()
            .key(key)
            .unwrap()
            .build()
            .expect("valid event")
    }

    fn ev_with_metadata(
        org: &str,
        topic: &str,
        namespace: &str,
        key: &str,
        metadata: crate::Metadata,
    ) -> Event {
        ev_with_key(org, topic, namespace, key).with_metadata(metadata)
    }

    #[test]
    fn subscription_keeps_consumer_identity_separate_from_filters() {
        let mut subscription = EventSubscription::new(OrganizationId::new("acme").unwrap());
        subscription.name = Some("billing-projection".to_owned());
        subscription.consumer_group_id = Some(crate::ConsumerGroupId::new("workers").unwrap());
        subscription.topics = Some(vec![Topic::new("invoice.created").unwrap()]);

        assert_eq!(subscription.name.as_deref(), Some("billing-projection"));
        assert_eq!(
            subscription
                .consumer_group_id
                .as_ref()
                .map(|id| id.as_str()),
            Some("workers")
        );
        assert!(subscription.matches(&ev("acme", "invoice.created", "/billing")));
        assert!(!subscription.matches(&ev("acme", "invoice.paid", "/billing")));
    }

    #[test]
    fn match_by_key_list() {
        let mut subscription = EventSubscription::new(OrganizationId::new("acme").unwrap());
        subscription.keys = Some(vec![crate::EventKey::new("invoice-1").unwrap()]);
        assert!(subscription.matches(&ev_with_key(
            "acme",
            "invoice.created",
            "/billing",
            "invoice-1"
        )));
        assert!(!subscription.matches(&ev_with_key(
            "acme",
            "invoice.created",
            "/billing",
            "invoice-2"
        )));
    }

    #[test]
    fn match_by_metadata_pairs() {
        let mut subscription = EventSubscription::new(OrganizationId::new("acme").unwrap());
        subscription.metadata = Some(crate::Metadata::new().with("tenant", "north").unwrap());
        assert!(
            subscription.matches(&ev_with_metadata(
                "acme",
                "invoice.created",
                "/billing",
                "invoice-1",
                crate::Metadata::new()
                    .with("tenant", "north")
                    .unwrap()
                    .with("trace", "abc")
                    .unwrap(),
            ))
        );
        assert!(!subscription.matches(&ev_with_metadata(
            "acme",
            "invoice.created",
            "/billing",
            "invoice-1",
            crate::Metadata::new().with("tenant", "south").unwrap(),
        )));
    }

    #[test]
    fn match_by_org() {
        let subscription = EventSubscription::new(OrganizationId::new("acme").unwrap());
        assert!(subscription.matches(&ev("acme", "thing.happened", "/x")));
        assert!(!subscription.matches(&ev("other", "thing.happened", "/x")));
    }

    #[test]
    fn match_by_topic_list() {
        let mut subscription = EventSubscription::new(OrganizationId::new("acme").unwrap());
        subscription.topics = Some(vec![Topic::new("a.b").unwrap(), Topic::new("c.d").unwrap()]);
        assert!(subscription.matches(&ev("acme", "a.b", "/x")));
        assert!(subscription.matches(&ev("acme", "c.d", "/x")));
        assert!(!subscription.matches(&ev("acme", "z.z", "/x")));
    }

    #[test]
    fn match_by_namespace_prefix() {
        let mut subscription = EventSubscription::new(OrganizationId::new("acme").unwrap());
        subscription.namespace_prefix = Some(Namespace::new("/backend").unwrap());
        assert!(subscription.matches(&ev("acme", "a.b", "/backend")));
        assert!(subscription.matches(&ev("acme", "a.b", "/backend/auth")));
        assert!(!subscription.matches(&ev("acme", "a.b", "/frontend")));
    }

    #[test]
    fn exclude_after_end_at() {
        let mut subscription = EventSubscription::new(OrganizationId::new("acme").unwrap());
        subscription.end_at = Some(Utc::now() - Duration::seconds(60));
        assert!(!subscription.matches(&ev("acme", "a.b", "/x")));
    }
}
