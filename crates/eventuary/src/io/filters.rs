use crate::event::Event;
use crate::io::Filter;
use crate::namespace::Namespace;
use crate::topic::Topic;

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
        Event::create("org", namespace, topic, "k", Payload::from_string("p")).unwrap()
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
}
