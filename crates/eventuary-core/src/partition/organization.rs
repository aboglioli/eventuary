use crate::error::Result;
use crate::event::Event;
use crate::partition::PartitionKeyResolver;

#[derive(Debug, Clone, Copy, Default)]
pub struct OrganizationPartitionKeyResolver;

impl PartitionKeyResolver for OrganizationPartitionKeyResolver {
    fn partition_key(&self, event: &Event) -> Result<String> {
        Ok(event.organization().as_str().to_owned())
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
        assert_eq!(resolver.partition_key(&event).unwrap(), "acme");
    }
}
