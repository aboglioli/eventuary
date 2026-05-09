# eventuary

Core event toolkit for logs, queues, streams, routing, replay, and acknowledgements.

This crate provides the event model (`Event`, `Topic`, `Namespace`, `OrganizationId`, `Payload`, `Metadata`, `EventCollector`, `SerializedEvent`, `ConsumerGroupId`, `StartFrom`) and a layer-agnostic error type. Backend implementations (memory, SQLite, Postgres, SQS, Kafka) live in sibling crates and depend on this one.
