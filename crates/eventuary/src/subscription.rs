use chrono::{DateTime, Utc};

use crate::{
    ConsumerGroupId, Event, EventKey, Metadata, Namespace, OrganizationId, StartFrom, Topic,
};

#[derive(Debug, Clone)]
pub struct EventSubscription {
    pub name: Option<String>,
    pub consumer_group_id: Option<ConsumerGroupId>,

    pub organization: OrganizationId,
    pub topics: Option<Vec<Topic>>,
    pub namespace_prefix: Option<Namespace>,
    pub keys: Option<Vec<EventKey>>,
    pub metadata: Option<Metadata>,
    pub start_from: StartFrom,
    pub end_at: Option<DateTime<Utc>>,
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
            && !keys.iter().any(|k| k == event.key())
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

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::Duration;

    use crate::Payload;

    fn ev(org: &str, topic: &str, namespace: &str) -> Event {
        ev_with_key(org, topic, namespace, "k")
    }

    fn ev_with_key(org: &str, topic: &str, namespace: &str, key: &str) -> Event {
        Event::create(org, namespace, topic, key, Payload::from_string("p")).unwrap()
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
