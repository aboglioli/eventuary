use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::Result;
use crate::event::Event;
use crate::payload::Payload;

pub trait PayloadCodec<P>: Clone + Send + Sync + 'static {
    fn encode(&self, payload: &P) -> Result<Payload>;
    fn decode(&self, payload: &Payload) -> Result<P>;
}

pub trait EventCodec<P>: Clone + Send + Sync + 'static {
    fn encode(&self, event: &Event<P>) -> Result<Event<Payload>>;
    fn decode(&self, event: Event<Payload>) -> Result<Event<P>>;
}

#[derive(Debug, Clone)]
pub struct PayloadEventCodec<C> {
    payload_codec: C,
}

impl<C> PayloadEventCodec<C> {
    pub fn new(payload_codec: C) -> Self {
        Self { payload_codec }
    }

    pub fn payload_codec(&self) -> &C {
        &self.payload_codec
    }
}

impl<P, C> EventCodec<P> for PayloadEventCodec<C>
where
    C: PayloadCodec<P>,
{
    fn encode(&self, event: &Event<P>) -> Result<Event<Payload>> {
        event.encode_payload(&self.payload_codec)
    }

    fn decode(&self, event: Event<Payload>) -> Result<Event<P>> {
        event.decode_payload(&self.payload_codec)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct JsonPayloadCodec;

impl<P> PayloadCodec<P> for JsonPayloadCodec
where
    P: Serialize + DeserializeOwned,
{
    fn encode(&self, payload: &P) -> Result<Payload> {
        Payload::from_json(payload)
    }

    fn decode(&self, payload: &Payload) -> Result<P> {
        payload.to_json()
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PayloadPassthroughCodec;

impl PayloadCodec<Payload> for PayloadPassthroughCodec {
    fn encode(&self, payload: &Payload) -> Result<Payload> {
        Ok(payload.clone())
    }

    fn decode(&self, payload: &Payload) -> Result<Payload> {
        Ok(payload.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::error::Error;

    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    struct UserUpdated {
        user_id: String,
        email: String,
    }

    #[test]
    fn json_codec_roundtrips_typed_payload() {
        let codec = JsonPayloadCodec;
        let typed = UserUpdated {
            user_id: "u-1".to_owned(),
            email: "a@example.com".to_owned(),
        };

        let payload = codec.encode(&typed).unwrap();
        assert_eq!(payload.content_type(), crate::ContentType::Json);

        let decoded: UserUpdated = codec.decode(&payload).unwrap();
        assert_eq!(decoded, typed);
    }

    #[test]
    fn json_codec_rejects_non_json_payload() {
        let codec = JsonPayloadCodec;
        let payload = Payload::from_string("not-json");
        let decoded: Result<serde_json::Value> = codec.decode(&payload);
        let err = decoded.unwrap_err();
        assert!(matches!(err, Error::InvalidPayload(_)));
    }

    #[test]
    fn payload_passthrough_clones_payload() {
        let codec = PayloadPassthroughCodec;
        let payload = Payload::from_string("hello");
        let encoded = codec.encode(&payload).unwrap();
        let decoded = codec.decode(&encoded).unwrap();
        assert_eq!(decoded.data(), b"hello");
        assert_eq!(decoded.content_type(), crate::ContentType::PlainText);
    }

    #[test]
    fn payload_event_codec_encodes_and_decodes_event() {
        let codec = PayloadEventCodec::new(JsonPayloadCodec);
        let event: Event<UserUpdated> = Event::create(
            "acme",
            "/users",
            "user.updated",
            "thing-1",
            UserUpdated {
                user_id: "u-1".to_owned(),
                email: "a@example.com".to_owned(),
            },
        )
        .unwrap();

        let id = event.id();
        let encoded = codec.encode(&event).unwrap();
        assert_eq!(encoded.id(), id);
        assert_eq!(encoded.payload().content_type(), crate::ContentType::Json);

        let decoded: Event<UserUpdated> = codec.decode(encoded).unwrap();
        assert_eq!(decoded.id(), id);
        assert_eq!(decoded.payload().email, "a@example.com");
    }
}
