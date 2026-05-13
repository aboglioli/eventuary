use crate::event::Event;
use crate::io::Filter;
use crate::topic::Topic;

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

impl Filter for TopicPattern {
    fn matches(&self, event: &Event) -> bool {
        self.matches_topic(event.topic())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::event::Event;
    use crate::io::Filter;
    use crate::payload::Payload;

    #[test]
    fn exact_matches_identical_topic() {
        let pattern = TopicPattern::exact(Topic::new("invoice.created").unwrap());
        assert!(pattern.matches_topic(&Topic::new("invoice.created").unwrap()));
        assert!(!pattern.matches_topic(&Topic::new("invoice.paid").unwrap()));
    }

    #[test]
    fn topic_pattern_is_a_filter() {
        let pattern = TopicPattern::exact(Topic::new("invoice.created").unwrap());
        let event =
            Event::create("org", "/x", "invoice.created", Payload::from_string("p")).unwrap();
        assert!(Filter::matches(&pattern, &event));
    }

    #[test]
    fn topic_pattern_filter_rejects_non_matching() {
        let pattern = TopicPattern::exact(Topic::new("invoice.created").unwrap());
        let event = Event::create("org", "/x", "invoice.paid", Payload::from_string("p")).unwrap();
        assert!(!Filter::matches(&pattern, &event));
    }
}
