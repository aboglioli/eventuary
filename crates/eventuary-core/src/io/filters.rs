use std::sync::Arc;

use chrono::{DateTime, Utc};

use crate::event::Event;
use crate::event_key::EventKey;
use crate::metadata::Metadata;
use crate::namespace::Namespace;
use crate::namespace_pattern::NamespacePattern;
use crate::organization::OrganizationId;
use crate::topic::Topic;
use crate::topic_pattern::TopicPattern;

pub trait Filter: Send + Sync {
    fn matches(&self, event: &Event) -> bool;
}

impl<T: Filter + ?Sized> Filter for Arc<T> {
    fn matches(&self, event: &Event) -> bool {
        (**self).matches(event)
    }
}

impl<T: Filter + ?Sized> Filter for Box<T> {
    fn matches(&self, event: &Event) -> bool {
        (**self).matches(event)
    }
}

pub type BoxFilter = Box<dyn Filter>;
pub type ArcFilter = Arc<dyn Filter>;

pub trait FilterExt: Filter + Sized + 'static {
    fn into_boxed(self) -> BoxFilter {
        Box::new(self)
    }

    fn into_arced(self) -> ArcFilter {
        Arc::new(self)
    }

    fn and<F: Filter + 'static>(self, other: F) -> AndFilter<Self, F> {
        AndFilter(self, other)
    }

    fn or<F: Filter + 'static>(self, other: F) -> OrFilter<Self, F> {
        OrFilter(self, other)
    }

    fn not(self) -> NotFilter<Self> {
        NotFilter(self)
    }
}

impl<T: Filter + 'static> FilterExt for T {}

/// Logical AND of two filters; matches when both inner filters match.
pub struct AndFilter<A: Filter, B: Filter>(pub A, pub B);

impl<A: Filter, B: Filter> Filter for AndFilter<A, B> {
    fn matches(&self, event: &Event) -> bool {
        self.0.matches(event) && self.1.matches(event)
    }
}

/// Logical OR of two filters; matches when either inner filter matches.
pub struct OrFilter<A: Filter, B: Filter>(pub A, pub B);

impl<A: Filter, B: Filter> Filter for OrFilter<A, B> {
    fn matches(&self, event: &Event) -> bool {
        self.0.matches(event) || self.1.matches(event)
    }
}

/// Logical NOT of a filter; matches when the inner filter does not match.
pub struct NotFilter<F: Filter>(pub F);

impl<F: Filter> Filter for NotFilter<F> {
    fn matches(&self, event: &Event) -> bool {
        !self.0.matches(event)
    }
}

impl<F: Filter> Filter for Vec<F> {
    fn matches(&self, event: &Event) -> bool {
        self.iter().any(|f| f.matches(event))
    }
}

pub struct AllFilter;

impl Filter for AllFilter {
    fn matches(&self, _: &Event) -> bool {
        true
    }
}

pub struct TopicFilter {
    topics: Vec<Topic>,
}

impl TopicFilter {
    pub fn new(topics: Vec<Topic>) -> Self {
        Self { topics }
    }
}

impl Filter for TopicFilter {
    fn matches(&self, event: &Event) -> bool {
        self.topics.iter().any(|t| t == event.topic())
    }
}

pub struct NamespacePrefixFilter {
    prefix: Namespace,
}

impl NamespacePrefixFilter {
    pub fn new(prefix: Namespace) -> Self {
        Self { prefix }
    }
}

impl Filter for NamespacePrefixFilter {
    fn matches(&self, event: &Event) -> bool {
        event.namespace().starts_with(&self.prefix)
    }
}

#[derive(Debug, Clone, Default)]
pub struct EventFilter {
    pub organization: Option<OrganizationId>,
    pub topics: Option<Vec<TopicPattern>>,
    pub namespace: Option<NamespacePattern>,
    pub keys: Option<Vec<EventKey>>,
    pub metadata: Option<Metadata>,
    pub end_at: Option<DateTime<Utc>>,
}

impl EventFilter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn for_organization(organization: OrganizationId) -> Self {
        Self {
            organization: Some(organization),
            ..Self::default()
        }
    }

    pub fn matches(&self, event: &Event) -> bool {
        if let Some(organization) = self.organization.as_ref()
            && event.organization() != organization
        {
            return false;
        }
        if let Some(topics) = self.topics.as_ref()
            && !topics.iter().any(|p| p.matches_topic(event.topic()))
        {
            return false;
        }
        if let Some(namespace) = self.namespace.as_ref()
            && !namespace.matches_namespace(event.namespace())
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

impl Filter for EventFilter {
    fn matches(&self, event: &Event) -> bool {
        EventFilter::matches(self, event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::payload::Payload;

    fn ev(topic: &str, namespace: &str) -> Event {
        Event::create("org", namespace, topic, Payload::from_string("p")).unwrap()
    }

    struct AllowAll;
    impl Filter for AllowAll {
        fn matches(&self, _: &Event) -> bool {
            true
        }
    }

    struct AllowNothing;
    impl Filter for AllowNothing {
        fn matches(&self, _: &Event) -> bool {
            false
        }
    }

    #[test]
    fn all_filter_matches_everything() {
        assert!(AllFilter.matches(&ev("a.b", "/x")));
    }

    #[test]
    fn topic_filter_matches_listed() {
        let f = TopicFilter::new(vec![Topic::new("task.created").unwrap()]);
        assert!(f.matches(&ev("task.created", "/x")));
        assert!(!f.matches(&ev("task.completed", "/x")));
    }

    #[test]
    fn namespace_prefix_filter() {
        let f = NamespacePrefixFilter::new(Namespace::new("/backend").unwrap());
        assert!(f.matches(&ev("a.b", "/backend")));
        assert!(f.matches(&ev("a.b", "/backend/auth")));
        assert!(!f.matches(&ev("a.b", "/frontend")));
    }

    #[test]
    fn filter_into_boxed_yields_dyn_filter() {
        let f: BoxFilter = AllowAll.into_boxed();
        assert!(f.matches(&ev("a.b", "/x")));
        let f: BoxFilter = AllowNothing.into_boxed();
        assert!(!f.matches(&ev("a.b", "/x")));
    }

    #[test]
    fn filter_into_arced_yields_shared_filter() {
        let f: ArcFilter = AllowAll.into_arced();
        let clone = Arc::clone(&f);
        assert!(f.matches(&ev("a.b", "/x")));
        assert!(clone.matches(&ev("a.b", "/x")));
    }

    #[test]
    fn filter_box_blanket_passes_as_generic_filter() {
        fn take<F: Filter>(f: F, e: &Event) -> bool {
            f.matches(e)
        }
        let boxed: BoxFilter = AllowAll.into_boxed();
        assert!(take(boxed, &ev("a.b", "/x")));
    }

    fn _assert_filter_dyn_safe() {
        fn _take(_: BoxFilter) {}
        fn _take_arc(_: ArcFilter) {}
    }

    #[test]
    fn and_filter_requires_both() {
        struct MatchA;
        struct MatchB;

        impl Filter for MatchA {
            fn matches(&self, e: &Event) -> bool {
                e.topic().as_str() == "a"
            }
        }
        impl Filter for MatchB {
            fn matches(&self, e: &Event) -> bool {
                e.namespace().as_str().starts_with("/x")
            }
        }

        let ab = MatchA.and(MatchB);
        assert!(ab.matches(&ev("a", "/x")));
        assert!(!ab.matches(&ev("a", "/y")));
        assert!(!ab.matches(&ev("b", "/x")));
    }

    #[test]
    fn or_filter_requires_either() {
        struct MatchA;
        struct MatchB;

        impl Filter for MatchA {
            fn matches(&self, e: &Event) -> bool {
                e.topic().as_str() == "a"
            }
        }
        impl Filter for MatchB {
            fn matches(&self, e: &Event) -> bool {
                e.namespace().as_str().starts_with("/x")
            }
        }

        let a_or_b = MatchA.or(MatchB);
        assert!(a_or_b.matches(&ev("a", "/y")));
        assert!(a_or_b.matches(&ev("b", "/x")));
        assert!(!a_or_b.matches(&ev("b", "/y")));
    }

    #[test]
    fn not_filter_inverts() {
        struct MatchA;
        impl Filter for MatchA {
            fn matches(&self, e: &Event) -> bool {
                e.topic().as_str() == "a"
            }
        }

        let not_a = MatchA.not();
        assert!(!not_a.matches(&ev("a", "/x")));
        assert!(not_a.matches(&ev("b", "/x")));
    }

    #[test]
    fn vec_filter_is_or() {
        struct MatchTopic(&'static str);
        impl Filter for MatchTopic {
            fn matches(&self, e: &Event) -> bool {
                e.topic().as_str() == self.0
            }
        }

        let filters: Vec<MatchTopic> = vec![MatchTopic("a"), MatchTopic("b")];
        assert!(filters.matches(&ev("a", "/x")));
        assert!(filters.matches(&ev("b", "/x")));
        assert!(!filters.matches(&ev("c", "/x")));
    }

    #[test]
    fn combinators_can_be_nested() {
        struct OrgMatch;
        impl Filter for OrgMatch {
            fn matches(&self, e: &Event) -> bool {
                e.organization().as_str() == "org"
            }
        }
        struct TopicA;
        impl Filter for TopicA {
            fn matches(&self, e: &Event) -> bool {
                e.topic().as_str() == "a"
            }
        }
        struct TopicB;
        impl Filter for TopicB {
            fn matches(&self, e: &Event) -> bool {
                e.topic().as_str() == "b"
            }
        }

        let filter = OrgMatch.and(TopicA.or(TopicB));
        assert!(filter.matches(&ev("a", "/x")));
        assert!(filter.matches(&ev("b", "/x")));
        assert!(!filter.matches(&ev("c", "/x")));
    }
}
