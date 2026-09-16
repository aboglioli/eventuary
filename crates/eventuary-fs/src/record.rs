use eventuary_core::{Error, Result, SerializedEvent};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub offset: u64,
    #[serde(flatten)]
    pub event: SerializedEvent,
}

impl Record {
    pub fn new(offset: u64, event: SerializedEvent) -> Self {
        Self { offset, event }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut line = serde_json::to_vec(self)
            .map_err(|e| Error::Serialization(format!("encode record: {e}")))?;
        line.push(b'\n');
        Ok(line)
    }

    pub fn decode(line: &[u8]) -> Result<Self> {
        serde_json::from_slice(line)
            .map_err(|e| Error::Serialization(format!("decode record: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use eventuary_core::{Event, Payload};

    use super::*;

    fn event() -> SerializedEvent {
        let event = Event::builder(
            "acme",
            "/orders",
            "order.created",
            "order-1",
            Payload::from_string("{}"),
        )
        .unwrap()
        .build()
        .unwrap();
        SerializedEvent::from_event(&event).unwrap()
    }

    #[test]
    fn encode_ends_with_newline_and_has_no_interior_newline() {
        let encoded = Record::new(7, event()).encode().unwrap();

        assert_eq!(encoded.last(), Some(&b'\n'));
        assert_eq!(encoded.iter().filter(|b| **b == b'\n').count(), 1);
    }

    #[test]
    fn round_trips_through_encode_decode() {
        let record = Record::new(42, event());

        let encoded = record.encode().unwrap();
        let decoded = Record::decode(&encoded[..encoded.len() - 1]).unwrap();

        assert_eq!(decoded.offset, 42);
        assert_eq!(decoded.event.id, record.event.id);
        assert_eq!(decoded.event.topic, record.event.topic);
    }

    #[test]
    fn offset_is_flat_in_the_json_object() {
        let encoded = Record::new(3, event()).encode().unwrap();
        let value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();

        assert_eq!(value["offset"], 3);
        assert_eq!(value["topic"], "order.created");
    }

    #[test]
    fn decode_rejects_malformed_line() {
        assert!(Record::decode(b"{not json").is_err());
    }
}
