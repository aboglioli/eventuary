use crate::topic::Topic;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TopicPattern {
    Exact(Topic),
}

impl TopicPattern {
    pub fn exact(topic: Topic) -> Self {
        Self::Exact(topic)
    }

    pub fn matches(&self, topic: &Topic) -> bool {
        match self {
            Self::Exact(expected) => expected == topic,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_matches_identical_topic() {
        let pattern = TopicPattern::exact(Topic::new("invoice.created").unwrap());
        assert!(pattern.matches(&Topic::new("invoice.created").unwrap()));
        assert!(!pattern.matches(&Topic::new("invoice.paid").unwrap()));
    }
}
