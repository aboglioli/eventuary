use super::{PartitionKey, PartitionKeyResolver, UnkeyedPartitionMode};
use crate::error::{Error, Result};
use crate::event::Event;

pub struct MetadataPartitionKeyResolver {
    field: String,
    unkeyed_mode: UnkeyedPartitionMode,
}

impl MetadataPartitionKeyResolver {
    pub fn new(field: impl Into<String>, mode: UnkeyedPartitionMode) -> Self {
        Self {
            field: field.into(),
            unkeyed_mode: mode,
        }
    }
}

impl<P: Send + Sync + 'static> PartitionKeyResolver<P> for MetadataPartitionKeyResolver {
    fn partition_key(&self, event: &Event<P>) -> Result<PartitionKey> {
        match event.metadata().get(&self.field) {
            Some(value) => PartitionKey::new(value),
            None => match self.unkeyed_mode {
                UnkeyedPartitionMode::Error => Err(Error::InvalidEventKey(format!(
                    "unkeyed event rejected by resolver: metadata field '{}' not found",
                    self.field
                ))),
                UnkeyedPartitionMode::EventId => {
                    PartitionKey::new(event.id().as_uuid().to_string())
                }
            },
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
            Payload::from_string("{}"),
        )
        .unwrap()
    }

    #[test]
    fn resolves_metadata_field() {
        let resolver = MetadataPartitionKeyResolver::new("tenant", UnkeyedPartitionMode::Error);
        let event = event_with_metadata("tenant", "acme-corp");
        assert_eq!(
            resolver.partition_key(&event).unwrap().as_str(),
            "acme-corp"
        );
    }

    #[test]
    fn error_mode_rejects_missing_field() {
        let resolver = MetadataPartitionKeyResolver::new("tenant", UnkeyedPartitionMode::Error);
        let event = event_without_metadata();
        let err = resolver.partition_key(&event).unwrap_err();
        assert!(matches!(err, Error::InvalidEventKey(_)));
    }

    #[test]
    fn event_id_mode_falls_back_to_uuid_string() {
        let resolver = MetadataPartitionKeyResolver::new("tenant", UnkeyedPartitionMode::EventId);
        let event = event_without_metadata();
        let key = resolver.partition_key(&event).unwrap();
        assert_eq!(key.as_str(), event.id().as_uuid().to_string());
    }
}
