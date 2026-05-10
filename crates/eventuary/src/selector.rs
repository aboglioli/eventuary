use chrono::{DateTime, Utc};

use crate::{Event, Namespace, OrganizationId, StartFrom, Topic};

#[derive(Debug, Clone)]
pub struct EventSelector {
    pub organization: OrganizationId,
    pub topics: Option<Vec<Topic>>,
    pub namespace_prefix: Option<Namespace>,
    pub start_from: StartFrom,
    pub end_at: Option<DateTime<Utc>>,
    pub limit: Option<usize>,
}

impl EventSelector {
    pub fn new(organization: OrganizationId) -> Self {
        Self {
            organization,
            topics: None,
            namespace_prefix: None,
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
        Event::create(org, namespace, topic, "k", Payload::from_string("p")).unwrap()
    }

    #[test]
    fn match_by_org() {
        let selector = EventSelector::new(OrganizationId::new("acme").unwrap());
        assert!(selector.matches(&ev("acme", "thing.happened", "/x")));
        assert!(!selector.matches(&ev("other", "thing.happened", "/x")));
    }

    #[test]
    fn match_by_topic_list() {
        let mut selector = EventSelector::new(OrganizationId::new("acme").unwrap());
        selector.topics = Some(vec![Topic::new("a.b").unwrap(), Topic::new("c.d").unwrap()]);
        assert!(selector.matches(&ev("acme", "a.b", "/x")));
        assert!(selector.matches(&ev("acme", "c.d", "/x")));
        assert!(!selector.matches(&ev("acme", "z.z", "/x")));
    }

    #[test]
    fn match_by_namespace_prefix() {
        let mut selector = EventSelector::new(OrganizationId::new("acme").unwrap());
        selector.namespace_prefix = Some(Namespace::new("/backend").unwrap());
        assert!(selector.matches(&ev("acme", "a.b", "/backend")));
        assert!(selector.matches(&ev("acme", "a.b", "/backend/auth")));
        assert!(!selector.matches(&ev("acme", "a.b", "/frontend")));
    }

    #[test]
    fn exclude_after_end_at() {
        let mut selector = EventSelector::new(OrganizationId::new("acme").unwrap());
        selector.end_at = Some(Utc::now() - Duration::seconds(60));
        assert!(!selector.matches(&ev("acme", "a.b", "/x")));
    }
}
