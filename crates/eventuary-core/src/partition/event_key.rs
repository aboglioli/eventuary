use crate::error::{Error, Result};
use crate::event::Event;
use crate::partition::types::PartitionKey;
use crate::partition::{PartitionKeyResolver, UnkeyedPartitionMode};

pub struct EventKeyPartitionKeyResolver {
    unkeyed_mode: UnkeyedPartitionMode,
}

impl EventKeyPartitionKeyResolver {
    pub fn with_unkeyed_mode(mode: UnkeyedPartitionMode) -> Self {
        Self { unkeyed_mode: mode }
    }

    pub fn error_on_unkeyed() -> Self {
        Self::with_unkeyed_mode(UnkeyedPartitionMode::Error)
    }

    pub fn event_id_on_unkeyed() -> Self {
        Self::with_unkeyed_mode(UnkeyedPartitionMode::EventId)
    }
}

impl Default for EventKeyPartitionKeyResolver {
    fn default() -> Self {
        Self::event_id_on_unkeyed()
    }
}

impl PartitionKeyResolver for EventKeyPartitionKeyResolver {
    fn partition_key(&self, event: &Event) -> Result<PartitionKey> {
        match event.key() {
            Some(k) => PartitionKey::new(k.as_str()),
            None => match self.unkeyed_mode {
                UnkeyedPartitionMode::Error => Err(Error::InvalidEventKey(
                    "unkeyed event rejected by resolver".to_owned(),
                )),
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
    use crate::payload::Payload;

    fn unkeyed_event() -> Event {
        Event::create(
            "acme",
            "/billing",
            "invoice.created",
            Payload::from_string("{}"),
        )
        .unwrap()
    }

    fn keyed_event() -> Event {
        Event::builder(
            "acme",
            "/billing",
            "invoice.created",
            Payload::from_string("{}"),
        )
        .unwrap()
        .key("invoice-123")
        .unwrap()
        .build()
        .unwrap()
    }

    #[test]
    fn resolves_key_when_present() {
        let resolver = EventKeyPartitionKeyResolver::default();
        let event = keyed_event();
        assert_eq!(
            resolver.partition_key(&event).unwrap().as_str(),
            "invoice-123"
        );
    }

    #[test]
    fn event_id_mode_falls_back_to_uuid_string() {
        let resolver = EventKeyPartitionKeyResolver::event_id_on_unkeyed();
        let event = unkeyed_event();
        let key = resolver.partition_key(&event).unwrap();
        assert_eq!(key.as_str(), event.id().as_uuid().to_string());
    }

    #[test]
    fn error_mode_rejects_unkeyed_event() {
        let resolver = EventKeyPartitionKeyResolver::error_on_unkeyed();
        let event = unkeyed_event();
        let err = resolver.partition_key(&event).unwrap_err();
        assert!(matches!(err, Error::InvalidEventKey(_)));
    }

    #[test]
    fn default_is_event_id_on_unkeyed() {
        let resolver = EventKeyPartitionKeyResolver::default();
        let event = unkeyed_event();
        assert!(resolver.partition_key(&event).is_ok());
    }
}
