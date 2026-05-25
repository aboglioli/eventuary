use super::{PartitionKey, PartitionKeyResolver};
use crate::error::Result;
use crate::event::Event;

#[derive(Debug, Clone, Copy, Default)]
pub struct EventKeyPartitionKeyResolver;

impl EventKeyPartitionKeyResolver {
    pub fn new() -> Self {
        Self
    }
}

impl<P: Send + Sync + 'static> PartitionKeyResolver<P> for EventKeyPartitionKeyResolver {
    fn partition_key(&self, event: &Event<P>) -> Result<PartitionKey> {
        PartitionKey::new(event.key().as_str())
    }
}

#[cfg(test)]
#[allow(clippy::default_constructed_unit_structs)]
mod tests {
    use super::*;
    use crate::event::Event;
    use crate::payload::Payload;

    fn keyed_event() -> Event {
        Event::create(
            "acme",
            "/billing",
            "invoice.created",
            "invoice-123",
            Payload::from_string("{}"),
        )
        .unwrap()
    }

    #[test]
    fn resolves_required_event_key() {
        let resolver = EventKeyPartitionKeyResolver::default();
        let event = keyed_event();
        assert_eq!(
            resolver.partition_key(&event).unwrap().as_str(),
            "invoice-123"
        );
    }

    #[test]
    fn new_matches_default() {
        let event = keyed_event();
        assert_eq!(
            EventKeyPartitionKeyResolver::new()
                .partition_key(&event)
                .unwrap()
                .as_str(),
            EventKeyPartitionKeyResolver::default()
                .partition_key(&event)
                .unwrap()
                .as_str()
        );
    }
}
