use super::{PartitionKey, PartitionKeyResolver};
use crate::error::{Error, Result};
use crate::event::Event;

pub struct MetadataPartitionKeyResolver {
    field: String,
}

impl MetadataPartitionKeyResolver {
    pub fn new(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
        }
    }
}

impl<P> PartitionKeyResolver<P> for MetadataPartitionKeyResolver {
    fn partition_key(&self, event: &Event<P>) -> Result<PartitionKey> {
        match event.metadata().get(&self.field) {
            Some(value) => PartitionKey::new(value),
            None => Err(Error::InvalidEventKey(format!(
                "metadata field '{}' not found",
                self.field
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::event::Event;
    use crate::metadata::Metadata;
    use crate::payload::Payload;

    fn event_with_metadata(field: &str, value: &str) -> Event {
        let metadata = Metadata::new().with(field, value).unwrap();
        Event::builder(
            "acme",
            "/billing",
            "invoice.created",
            "invoice-123",
            Payload::from_string("{}"),
        )
        .unwrap()
        .metadata(metadata)
        .build()
        .unwrap()
    }

    fn event_without_metadata() -> Event {
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
    fn resolves_metadata_field() {
        let resolver = MetadataPartitionKeyResolver::new("tenant");
        let event = event_with_metadata("tenant", "acme-corp");
        assert_eq!(
            resolver.partition_key(&event).unwrap().as_str(),
            "acme-corp"
        );
    }

    #[test]
    fn rejects_missing_field() {
        let resolver = MetadataPartitionKeyResolver::new("tenant");
        let event = event_without_metadata();
        let err = resolver.partition_key(&event).unwrap_err();
        assert!(matches!(err, Error::InvalidEventKey(_)));
    }
}
