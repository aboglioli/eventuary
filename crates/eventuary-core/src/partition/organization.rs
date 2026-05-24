use super::{PartitionKey, PartitionKeyResolver};
use crate::error::Result;
use crate::event::Event;

#[derive(Debug, Clone, Copy, Default)]
pub struct OrganizationPartitionKeyResolver;

impl<P: Send + Sync + 'static> PartitionKeyResolver<P> for OrganizationPartitionKeyResolver {
    fn partition_key(&self, event: &Event<P>) -> Result<PartitionKey> {
        PartitionKey::new(event.organization().as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Event;
    use crate::payload::Payload;

    #[test]
    fn resolves_organization() {
        let resolver = OrganizationPartitionKeyResolver;
        let event = Event::create(
            "acme",
            "/billing",
            "invoice.created",
            Payload::from_string("{}"),
        )
        .unwrap();
        assert_eq!(resolver.partition_key(&event).unwrap().as_str(), "acme");
    }
}
