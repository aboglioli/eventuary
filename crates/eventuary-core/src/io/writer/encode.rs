use std::marker::PhantomData;

use crate::error::Result;
use crate::event::Event;
use crate::io::Writer;
use crate::payload::Payload;
use crate::payload_codec::{EventCodec, PayloadCodec, PayloadEventCodec};

/// Wraps a `Writer<Payload>` and encodes typed events using an `EventCodec<P>`.
pub struct EncodeWriter<W, C, P> {
    inner: W,
    codec: C,
    _payload: PhantomData<P>,
}

impl<W, C, P> EncodeWriter<W, C, P>
where
    C: EventCodec<P>,
{
    pub fn new(inner: W, codec: C) -> Self {
        Self {
            inner,
            codec,
            _payload: PhantomData,
        }
    }
}

impl<W, C, P> EncodeWriter<W, PayloadEventCodec<C>, P>
where
    C: PayloadCodec<P>,
{
    pub fn from_payload_codec(inner: W, codec: C) -> Self {
        Self::new(inner, PayloadEventCodec::new(codec))
    }
}

impl<W, C, P> Writer<P> for EncodeWriter<W, C, P>
where
    W: Writer<Payload>,
    C: EventCodec<P>,
    P: Send + Sync,
{
    async fn write(&self, event: &Event<P>) -> Result<()> {
        let encoded = self.codec.encode(event)?;
        self.inner.write(&encoded).await
    }

    async fn write_all(&self, events: &[Event<P>]) -> Result<()> {
        let encoded: Vec<Event<Payload>> = events
            .iter()
            .map(|event| self.codec.encode(event))
            .collect::<Result<_>>()?;
        self.inner.write_all(&encoded).await
    }
}

/// Extension methods for `Writer<Payload>` to add typed encoding.
pub trait WriterTypedExt: Writer<Payload> + Sized {
    fn encode<P, C>(self, codec: C) -> EncodeWriter<Self, PayloadEventCodec<C>, P>
    where
        C: PayloadCodec<P>,
    {
        EncodeWriter::from_payload_codec(self, codec)
    }

    fn encode_event<P, C>(self, codec: C) -> EncodeWriter<Self, C, P>
    where
        C: EventCodec<P>,
    {
        EncodeWriter::new(self, codec)
    }
}

impl<T: Writer<Payload> + Sized> WriterTypedExt for T {}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::{Arc, Mutex};

    use crate::Error;
    use crate::event::Event;
    use crate::io::Writer;
    use crate::payload::Payload as WirePayload;
    use crate::payload_codec::{EventCodec, JsonPayloadCodec};

    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    struct UserUpdated {
        user_id: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    struct UserDeleted {
        user_id: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum DomainEvent {
        UserUpdated(UserUpdated),
        UserDeleted(UserDeleted),
    }

    #[derive(Debug, Clone, Copy)]
    struct DomainEventCodec;

    impl EventCodec<DomainEvent> for DomainEventCodec {
        fn encode(&self, event: &Event<DomainEvent>) -> Result<Event<WirePayload>> {
            match event.payload() {
                DomainEvent::UserUpdated(payload) => event
                    .clone()
                    .map_payload(|_| payload.clone())
                    .encode_payload(&JsonPayloadCodec),
                DomainEvent::UserDeleted(payload) => event
                    .clone()
                    .map_payload(|_| payload.clone())
                    .encode_payload(&JsonPayloadCodec),
            }
        }

        fn decode(&self, event: Event<WirePayload>) -> Result<Event<DomainEvent>> {
            match event.topic().as_str() {
                "user.updated" => event
                    .decode_payload::<UserUpdated, _>(&JsonPayloadCodec)
                    .map(|event| event.map_payload(DomainEvent::UserUpdated)),
                "user.deleted" => event
                    .decode_payload::<UserDeleted, _>(&JsonPayloadCodec)
                    .map(|event| event.map_payload(DomainEvent::UserDeleted)),
                topic => Err(Error::InvalidPayload(format!(
                    "unsupported domain event topic: {topic}"
                ))),
            }
        }
    }

    #[derive(Clone, Default)]
    struct CapturingPayloadWriter {
        events: Arc<Mutex<Vec<Event<WirePayload>>>>,
    }

    impl Writer<WirePayload> for CapturingPayloadWriter {
        async fn write(&self, event: &Event<WirePayload>) -> Result<()> {
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }
    }

    #[tokio::test]
    async fn encode_writer_converts_typed_event_to_payload_event() {
        let inner = CapturingPayloadWriter::default();
        let captured = Arc::clone(&inner.events);
        let writer = EncodeWriter::<_, _, UserUpdated>::from_payload_codec(inner, JsonPayloadCodec);

        let event = Event::create(
            "acme",
            "/users",
            "user.updated",
            "thing-1",
            UserUpdated {
                user_id: "u-1".to_owned(),
            },
        )
        .unwrap();

        writer.write(&event).await.unwrap();

        let events = captured.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload().content_type(), crate::ContentType::Json);
        let decoded: UserUpdated = events[0].payload().to_json().unwrap();
        assert_eq!(decoded.user_id, "u-1");
    }

    #[tokio::test]
    async fn encode_writer_accepts_event_codec_for_domain_enum() {
        let inner = CapturingPayloadWriter::default();
        let captured = Arc::clone(&inner.events);
        let writer = EncodeWriter::<_, _, DomainEvent>::new(inner, DomainEventCodec);

        let event = Event::create(
            "acme",
            "/users",
            "user.deleted",
            "thing-1",
            DomainEvent::UserDeleted(UserDeleted {
                user_id: "u-1".to_owned(),
            }),
        )
        .unwrap();

        writer.write(&event).await.unwrap();

        let events = captured.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].topic().as_str(), "user.deleted");
        let decoded: UserDeleted = events[0].payload().to_json().unwrap();
        assert_eq!(decoded.user_id, "u-1");
    }
}
