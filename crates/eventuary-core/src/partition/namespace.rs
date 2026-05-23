use crate::error::Result;
use crate::event::Event;
use crate::partition::PartitionKeyResolver;

#[derive(Debug, Clone, Copy, Default)]
pub struct NamespacePartitionKeyResolver;

impl PartitionKeyResolver for NamespacePartitionKeyResolver {
    fn partition_key(&self, event: &Event) -> Result<String> {
        Ok(event.namespace().as_str().to_owned())
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
            Payload::from_string("{}"),
        )
        .unwrap();
        assert_eq!(resolver.partition_key(&event).unwrap(), "/billing");
    }

    #[test]
    fn resolves_nested_namespace() {
        let resolver = NamespacePartitionKeyResolver;
        let event = Event::create(
            "acme",
            "/billing/invoices",
            "invoice.created",
            Payload::from_string("{}"),
        )
        .unwrap();
        assert_eq!(resolver.partition_key(&event).unwrap(), "/billing/invoices");
    }
}
