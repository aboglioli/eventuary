use super::{PartitionKey, PartitionKeyResolver};
use crate::error::Result;
use crate::event::Event;

#[derive(Debug, Clone, Copy, Default)]
pub struct NamespacePartitionKeyResolver;

impl<P: Send + Sync + 'static> PartitionKeyResolver<P> for NamespacePartitionKeyResolver {
    fn partition_key(&self, event: &Event<P>) -> Result<PartitionKey> {
        PartitionKey::new(event.namespace().as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Event;
    use crate::payload::Payload;

    #[test]
    fn resolves_namespace() {
        let resolver = NamespacePartitionKeyResolver;
        let event = Event::create(
            "acme",
            "/billing",
            "invoice.created",
            "invoice-123",
            Payload::from_string("{}"),
        )
        .unwrap();
        assert_eq!(resolver.partition_key(&event).unwrap().as_str(), "/billing");
    }

    #[test]
    fn resolves_nested_namespace() {
        let resolver = NamespacePartitionKeyResolver;
        let event = Event::create(
            "acme",
            "/billing/invoices",
            "invoice.created",
            "invoice-123",
            Payload::from_string("{}"),
        )
        .unwrap();
        assert_eq!(
            resolver.partition_key(&event).unwrap().as_str(),
            "/billing/invoices"
        );
    }
}
