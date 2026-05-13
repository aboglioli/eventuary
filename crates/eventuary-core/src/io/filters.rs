use std::sync::Arc;

use crate::event::Event;
use crate::namespace::Namespace;
use crate::topic::Topic;

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
}

impl<T: Filter + 'static> FilterExt for T {}

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
}
