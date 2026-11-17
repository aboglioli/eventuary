use serde::Serialize;

pub trait DomainEvent: Serialize {
    const NAMESPACE: &'static str;
    const TOPIC: &'static str;
    const SCHEMA_VERSION: u32 = 1;

    fn key(&self) -> String;
}

/// Implements [`DomainEvent`] for a payload struct.
///
/// `key` names the field, or a dotted path of fields, whose `to_string()` is
/// the event key. `schema` sets `SCHEMA_VERSION` when it is not 1.
///
/// ```
/// use eventuary_core::{DomainEvent, domain_event};
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct OrderPlaced {
///     order_id: String,
/// }
///
/// domain_event!(OrderPlaced, "/orders", "order.placed", key = order_id);
///
/// assert_eq!(OrderPlaced::TOPIC, "order.placed");
/// assert_eq!(OrderPlaced { order_id: "o-1".into() }.key(), "o-1");
/// ```
#[macro_export]
macro_rules! domain_event {
    ($event:ident, $namespace:expr, $topic:expr, key = $($key:ident).+) => {
        impl $crate::DomainEvent for $event {
            const NAMESPACE: &'static str = $namespace;
            const TOPIC: &'static str = $topic;

            fn key(&self) -> String {
                self.$($key).+.to_string()
            }
        }
    };
    ($event:ident, $namespace:expr, $topic:expr, schema = $schema_version:expr, key = $($key:ident).+) => {
        impl $crate::DomainEvent for $event {
            const NAMESPACE: &'static str = $namespace;
            const TOPIC: &'static str = $topic;
            const SCHEMA_VERSION: u32 = $schema_version;

            fn key(&self) -> String {
                self.$($key).+.to_string()
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use super::*;
    use crate::{EventCollector, OrganizationId};

    #[derive(Serialize)]
    struct Header {
        order_id: u32,
    }

    #[derive(Serialize)]
    struct OrderPlaced {
        order_id: String,
    }

    #[derive(Serialize)]
    struct OrderShipped {
        header: Header,
    }

    domain_event!(OrderPlaced, "/orders", "order.placed", key = order_id);
    domain_event!(
        OrderShipped,
        "/orders",
        "order.shipped",
        schema = 3,
        key = header.order_id
    );

    #[test]
    fn a_declared_event_has_its_namespace_topic_default_schema_and_key() {
        let event = OrderPlaced {
            order_id: "o-1".to_owned(),
        };

        assert_eq!(OrderPlaced::NAMESPACE, "/orders");
        assert_eq!(OrderPlaced::TOPIC, "order.placed");
        assert_eq!(OrderPlaced::SCHEMA_VERSION, 1);
        assert_eq!(event.key(), "o-1");
    }

    #[test]
    fn a_schema_version_and_a_nested_key_can_be_declared() {
        let event = OrderShipped {
            header: Header { order_id: 7 },
        };

        assert_eq!(OrderShipped::SCHEMA_VERSION, 3);
        assert_eq!(event.key(), "7");
    }

    #[test]
    fn a_declared_event_is_recorded_with_its_declaration() {
        let mut collector = EventCollector::new(OrganizationId::platform());

        collector
            .record(
                OrderShipped {
                    header: Header { order_id: 9 },
                },
                chrono::Utc::now(),
            )
            .unwrap();

        let changes = collector.take_changes().unwrap();
        let event = &changes.events()[0];
        assert_eq!(event.namespace().as_str(), "/orders");
        assert_eq!(event.topic().as_str(), "order.shipped");
        assert_eq!(event.key().as_str(), "9");
        assert_eq!(event.schema_version().value(), 3);
    }
}
