use std::sync::Arc;

use crate::error::Result;
use crate::event::Event;
use crate::partition::PartitionKeyResolver;
use crate::partition::types::PartitionKey;
use crate::payload::Payload;

pub struct CompositePartitionKeyResolver<P = Payload> {
    parts: Vec<Arc<dyn PartitionKeyResolver<P>>>,
    separator: String,
}

impl<P> CompositePartitionKeyResolver<P> {
    pub fn new(parts: Vec<Arc<dyn PartitionKeyResolver<P>>>) -> Self {
        Self {
            parts,
            separator: ":".to_owned(),
        }
    }

    pub fn with_separator(mut self, sep: impl Into<String>) -> Self {
        self.separator = sep.into();
        self
    }
}

impl<P: Send + Sync + 'static> PartitionKeyResolver<P> for CompositePartitionKeyResolver<P> {
    fn partition_key(&self, event: &Event<P>) -> Result<PartitionKey> {
        let segments = self
            .parts
            .iter()
            .map(|r| r.partition_key(event))
            .collect::<Result<Vec<_>>>()?;
        let joined = segments
            .iter()
            .map(|k| k.as_str())
            .collect::<Vec<_>>()
            .join(&self.separator);
        PartitionKey::new(joined)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Event;
    use crate::partition::{OrganizationPartitionKeyResolver, TopicPartitionKeyResolver};
    use crate::payload::Payload;

    fn test_event() -> Event {
        Event::create(
            "acme",
            "/billing",
            "invoice.created",
            Payload::from_string("{}"),
        )
        .unwrap()
    }

    #[test]
    fn joins_two_resolvers_with_default_separator() {
        let resolver = CompositePartitionKeyResolver::new(vec![
            Arc::new(OrganizationPartitionKeyResolver),
            Arc::new(TopicPartitionKeyResolver),
        ]);
        let event = test_event();
        assert_eq!(
            resolver.partition_key(&event).unwrap().as_str(),
            "acme:invoice.created"
        );
    }

    #[test]
    fn custom_separator_is_used() {
        let resolver = CompositePartitionKeyResolver::new(vec![
            Arc::new(OrganizationPartitionKeyResolver),
            Arc::new(TopicPartitionKeyResolver),
        ])
        .with_separator("|");
        let event = test_event();
        assert_eq!(
            resolver.partition_key(&event).unwrap().as_str(),
            "acme|invoice.created"
        );
    }

    #[test]
    fn propagates_child_error() {
        use crate::partition::{EventKeyPartitionKeyResolver, UnkeyedPartitionMode};

        let resolver = CompositePartitionKeyResolver::new(vec![
            Arc::new(OrganizationPartitionKeyResolver),
            Arc::new(EventKeyPartitionKeyResolver::with_unkeyed_mode(
                UnkeyedPartitionMode::Error,
            )),
        ]);
        let event = test_event();
        assert!(resolver.partition_key(&event).is_err());
    }
}
