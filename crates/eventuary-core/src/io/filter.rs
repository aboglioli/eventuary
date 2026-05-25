use std::marker::PhantomData;
use std::sync::Arc;

use chrono::{DateTime, Utc};

use crate::event::Event;
use crate::event_key::EventKey;
use crate::metadata::Metadata;
use crate::namespace::Namespace;
use crate::organization::OrganizationId;
use crate::payload::Payload;
use crate::topic::Topic;

pub trait Filter<P = Payload>: Send + Sync {
    fn matches(&self, event: &Event<P>) -> bool;
}

impl<T: Filter<P> + ?Sized, P: Send + Sync> Filter<P> for Arc<T> {
    fn matches(&self, event: &Event<P>) -> bool {
        (**self).matches(event)
    }
}

impl<T: Filter<P> + ?Sized, P: Send + Sync> Filter<P> for Box<T> {
    fn matches(&self, event: &Event<P>) -> bool {
        (**self).matches(event)
    }
}

pub type BoxFilter<P = Payload> = Box<dyn Filter<P>>;
pub type ArcFilter<P = Payload> = Arc<dyn Filter<P>>;

pub trait FilterExt<P = Payload>: Filter<P> + Sized + 'static
where
    P: Send + Sync,
{
    fn into_boxed(self) -> BoxFilter<P> {
        Box::new(self)
    }

    fn into_arced(self) -> ArcFilter<P> {
        Arc::new(self)
    }

    fn and<F: Filter<P> + 'static>(self, other: F) -> AndFilter<Self, F, P>
    where
        P: 'static,
    {
        AndFilter(self, other, PhantomData)
    }

    fn or<F: Filter<P> + 'static>(self, other: F) -> OrFilter<Self, F, P>
    where
        P: 'static,
    {
        OrFilter(self, other, PhantomData)
    }

    fn not(self) -> NotFilter<Self, P>
    where
        P: 'static,
    {
        NotFilter(self, PhantomData)
    }
}

impl<T: Filter<P> + Sized + 'static, P: Send + Sync + 'static> FilterExt<P> for T {}

#[derive(Clone)]
pub struct AndFilter<A: Filter<P>, B: Filter<P>, P>(pub A, pub B, PhantomData<P>);

impl<A: Filter<P>, B: Filter<P>, P: Send + Sync> Filter<P> for AndFilter<A, B, P> {
    fn matches(&self, event: &Event<P>) -> bool {
        self.0.matches(event) && self.1.matches(event)
    }
}

#[derive(Clone)]
pub struct OrFilter<A: Filter<P>, B: Filter<P>, P>(pub A, pub B, PhantomData<P>);

impl<A: Filter<P>, B: Filter<P>, P: Send + Sync> Filter<P> for OrFilter<A, B, P> {
    fn matches(&self, event: &Event<P>) -> bool {
        self.0.matches(event) || self.1.matches(event)
    }
}

#[derive(Clone)]
pub struct NotFilter<F: Filter<P>, P>(pub F, PhantomData<P>);

impl<F: Filter<P>, P: Send + Sync> Filter<P> for NotFilter<F, P> {
    fn matches(&self, event: &Event<P>) -> bool {
        !self.0.matches(event)
    }
}

impl<F: Filter<P>, P: Send + Sync> Filter<P> for Vec<F> {
    fn matches(&self, event: &Event<P>) -> bool {
        self.iter().any(|f| f.matches(event))
    }
}

pub struct AllFilter;

impl<P: Send + Sync> Filter<P> for AllFilter {
    fn matches(&self, _: &Event<P>) -> bool {
        true
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TopicPattern {
    Exact(Topic),
}

impl TopicPattern {
    pub fn exact(topic: Topic) -> Self {
        Self::Exact(topic)
    }

    pub fn matches_topic(&self, topic: &Topic) -> bool {
        match self {
            Self::Exact(expected) => expected == topic,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum NamespacePattern {
    Prefix(Namespace),
}

impl NamespacePattern {
    pub fn prefix(namespace: Namespace) -> Self {
        Self::Prefix(namespace)
    }

    pub fn matches_namespace(&self, namespace: &Namespace) -> bool {
        match self {
            Self::Prefix(prefix) => namespace.starts_with(prefix),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct EventFilter {
    pub organization: Option<OrganizationId>,
    pub topic: Option<TopicPattern>,
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
}

impl<P: Send + Sync> Filter<P> for EventFilter {
    fn matches(&self, event: &Event<P>) -> bool {
        if let Some(organization) = self.organization.as_ref()
            && event.organization() != organization
        {
            return false;
        }
        if let Some(topic) = self.topic.as_ref()
            && !topic.matches_topic(event.topic())
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

impl<P: Send + Sync> Filter<P> for TopicPattern {
    fn matches(&self, event: &Event<P>) -> bool {
        self.matches_topic(event.topic())
    }
}

impl<P: Send + Sync> Filter<P> for NamespacePattern {
    fn matches(&self, event: &Event<P>) -> bool {
        self.matches_namespace(event.namespace())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::Namespace;
    use crate::Topic;

    fn ev(topic: &str, namespace: &str) -> Event {
        Event::create(
            "org",
            namespace,
            topic,
            crate::payload::Payload::from_string("p"),
        )
        .unwrap()
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

        let filters = vec![MatchTopic("a"), MatchTopic("b")];

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

    #[test]
    fn topic_pattern_exact_matches_identical_topic() {
        let pattern = TopicPattern::exact(Topic::new("invoice.created").unwrap());
        assert!(pattern.matches_topic(&Topic::new("invoice.created").unwrap()));
        assert!(!pattern.matches_topic(&Topic::new("invoice.paid").unwrap()));
    }

    #[test]
    fn namespace_pattern_prefix_matches_descendants() {
        let pattern = NamespacePattern::prefix(Namespace::new("/billing").unwrap());
        assert!(pattern.matches_namespace(&Namespace::new("/billing").unwrap()));
        assert!(pattern.matches_namespace(&Namespace::new("/billing/invoices").unwrap()));
        assert!(!pattern.matches_namespace(&Namespace::new("/orders").unwrap()));
    }

    #[test]
    fn topic_pattern_is_a_filter_through_io() {
        let pattern = TopicPattern::exact(Topic::new("invoice.created").unwrap());
        let event = Event::create(
            "org",
            "/x",
            "invoice.created",
            crate::payload::Payload::from_string("p"),
        )
        .unwrap();
        assert!(Filter::matches(&pattern, &event));
    }

    #[test]
    fn namespace_pattern_is_a_filter_through_io() {
        let pattern = NamespacePattern::prefix(Namespace::new("/billing").unwrap());
        let event = Event::create(
            "org",
            "/billing/invoices",
            "invoice.created",
            crate::payload::Payload::from_string("p"),
        )
        .unwrap();
        assert!(Filter::matches(&pattern, &event));
    }
}
