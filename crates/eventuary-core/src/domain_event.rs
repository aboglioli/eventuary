use serde::Serialize;

pub trait DomainEvent: Serialize {
    const NAMESPACE: &'static str;
    const TOPIC: &'static str;
    const SCHEMA_VERSION: u32 = 1;

    fn key(&self) -> String;
}
