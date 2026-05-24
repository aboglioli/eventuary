use std::marker::PhantomData;
use std::pin::Pin;

use futures::{StreamExt, stream};

use crate::error::Result;
use crate::io::message::Message;
use crate::io::{Acker, BoxStream, Reader};
use crate::payload::Payload;
use crate::payload_codec::{EventCodec, PayloadCodec, PayloadEventCodec};

/// Wraps a `Reader<Payload>` and decodes each message using an `EventCodec<P>`,
/// yielding typed `Reader<P>`.
pub struct DecodeReader<R, C, P> {
    inner: R,
    codec: C,
    disposition: DecodeErrorDisposition,
    _payload: PhantomData<P>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DecodeErrorDisposition {
    Surface,
    AckInner,
    #[default]
    NackInner,
}

impl<R, C, P> DecodeReader<R, C, P> {
    pub fn new(inner: R, codec: C) -> Self {
        Self {
            inner,
            codec,
            disposition: DecodeErrorDisposition::default(),
            _payload: PhantomData,
        }
    }

    pub fn with_disposition(mut self, disposition: DecodeErrorDisposition) -> Self {
        self.disposition = disposition;
        self
    }
}

impl<R, C, P> DecodeReader<R, PayloadEventCodec<C>, P>
where
    C: PayloadCodec<P>,
{
    pub fn from_payload_codec(inner: R, codec: C) -> Self {
        Self::new(inner, PayloadEventCodec::new(codec))
    }
}

impl<R, C, P> Reader<P> for DecodeReader<R, C, P>
where
    R: Reader<Payload> + Send + Sync + 'static,
    R::Subscription: Send + 'static,
    R::Acker: Acker + Send + Sync + 'static,
    R::Cursor: Send + Sync + 'static,
    R::Stream: 'static,
    C: EventCodec<P> + Clone + Send + Sync + 'static,
    P: Send + Sync + 'static,
{
    type Subscription = R::Subscription;
    type Acker = R::Acker;
    type Cursor = R::Cursor;
    type Stream = BoxStream<R::Cursor, R::Acker, P>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let inner = self.inner.read(subscription).await?;
        let state = DecodeState {
            inner: Box::pin(inner),
            codec: self.codec.clone(),
            disposition: self.disposition,
            _payload: PhantomData,
        };
        Ok(Box::pin(stream::unfold(state, next_decoded::<R, C, P>)))
    }
}

struct DecodeState<R, C, P>
where
    R: Reader<Payload>,
{
    inner: Pin<Box<R::Stream>>,
    codec: C,
    disposition: DecodeErrorDisposition,
    _payload: PhantomData<P>,
}

async fn next_decoded<R, C, P>(
    mut state: DecodeState<R, C, P>,
) -> Option<(
    Result<Message<R::Acker, R::Cursor, P>>,
    DecodeState<R, C, P>,
)>
where
    R: Reader<Payload>,
    C: EventCodec<P>,
{
    let item = state.inner.as_mut().next().await?;
    match item {
        Ok(msg) => {
            let (event, acker, cursor) = msg.into_parts();
            match state.codec.decode(event) {
                Ok(decoded) => Some((Ok(Message::new(decoded, acker, cursor)), state)),
                Err(e) => {
                    let disposition_result = match state.disposition {
                        DecodeErrorDisposition::Surface => Ok(()),
                        DecodeErrorDisposition::AckInner => acker.ack().await,
                        DecodeErrorDisposition::NackInner => acker.nack().await,
                    };
                    match disposition_result {
                        Ok(()) => Some((Err(e), state)),
                        Err(ack_err) => Some((Err(ack_err), state)),
                    }
                }
            }
        }
        Err(e) => Some((Err(e), state)),
    }
}

/// Extension methods for `Reader<Payload>` to add typed decoding.
pub trait ReaderTypedExt: Reader<Payload> + Sized {
    fn decode<P, C>(self, codec: C) -> DecodeReader<Self, PayloadEventCodec<C>, P>
    where
        C: PayloadCodec<P>,
    {
        DecodeReader::from_payload_codec(self, codec)
    }

    fn decode_event<P, C>(self, codec: C) -> DecodeReader<Self, C, P>
    where
        C: EventCodec<P>,
    {
        DecodeReader::new(self, codec)
    }
}

impl<T: Reader<Payload> + Sized> ReaderTypedExt for T {}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::{Arc, Mutex};

    use futures::Stream;
    use futures::StreamExt;
    use futures::stream;

    use crate::error::Error;
    use crate::event::Event;
    use crate::io::NoCursor;
    use crate::io::Writer;
    use crate::io::acker::NoopAcker;
    use crate::io::writer::EncodeWriter;
    use crate::io::writer::WriterTypedExt;
    use crate::payload::Payload as WirePayload;

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
                    .encode_payload(&crate::JsonPayloadCodec),
                DomainEvent::UserDeleted(payload) => event
                    .clone()
                    .map_payload(|_| payload.clone())
                    .encode_payload(&crate::JsonPayloadCodec),
            }
        }

        fn decode(&self, event: Event<WirePayload>) -> Result<Event<DomainEvent>> {
            match event.topic().as_str() {
                "user.updated" => event
                    .decode_payload::<UserUpdated, _>(&crate::JsonPayloadCodec)
                    .map(|event| event.map_payload(DomainEvent::UserUpdated)),
                "user.deleted" => event
                    .decode_payload::<UserDeleted, _>(&crate::JsonPayloadCodec)
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
        let writer =
            EncodeWriter::<_, _, UserUpdated>::from_payload_codec(inner, crate::JsonPayloadCodec);

        let event = Event::create(
            "acme",
            "/users",
            "user.updated",
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

    #[derive(Debug, Clone, Default)]
    struct TestSub;

    struct VecPayloadReader {
        events: Vec<Event<WirePayload>>,
    }

    impl VecPayloadReader {
        fn new(events: Vec<Event<WirePayload>>) -> Self {
            Self { events }
        }
    }

    impl Reader<WirePayload> for VecPayloadReader {
        type Subscription = TestSub;
        type Acker = NoopAcker;
        type Cursor = NoCursor;
        type Stream =
            Pin<Box<dyn Stream<Item = Result<Message<NoopAcker, NoCursor, WirePayload>>> + Send>>;

        async fn read(&self, _: Self::Subscription) -> Result<Self::Stream> {
            let events = self
                .events
                .clone()
                .into_iter()
                .map(|event| Ok(Message::new(event, NoopAcker, NoCursor)))
                .collect::<Vec<_>>();
            Ok(Box::pin(stream::iter(events)))
        }
    }

    #[tokio::test]
    async fn decode_reader_converts_payload_event_to_typed_event() {
        let raw = VecPayloadReader::new(vec![
            Event::create(
                "acme",
                "/users",
                "user.updated",
                WirePayload::from_json(&UserUpdated {
                    user_id: "u-1".to_owned(),
                })
                .unwrap(),
            )
            .unwrap(),
        ]);

        let reader =
            DecodeReader::<_, _, UserUpdated>::from_payload_codec(raw, crate::JsonPayloadCodec);
        let mut stream = reader.read(TestSub).await.unwrap();
        let msg = stream.next().await.unwrap().unwrap();

        assert_eq!(msg.event().payload().user_id, "u-1");
    }

    #[derive(Debug, Clone, Default)]
    struct CountingAcker {
        ack_count: Arc<std::sync::atomic::AtomicUsize>,
        nack_count: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Acker for CountingAcker {
        async fn ack(&self) -> Result<()> {
            self.ack_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        async fn nack(&self) -> Result<()> {
            self.nack_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn decode_reader_nacks_inner_on_decode_failure_by_default() {
        let acker = CountingAcker::default();
        let raw = {
            struct TypedVecReader {
                events: Vec<Message<CountingAcker, NoCursor, WirePayload>>,
            }
            impl Reader<WirePayload> for TypedVecReader {
                type Subscription = TestSub;
                type Acker = CountingAcker;
                type Cursor = NoCursor;
                type Stream = Pin<
                    Box<
                        dyn Stream<Item = Result<Message<CountingAcker, NoCursor, WirePayload>>>
                            + Send,
                    >,
                >;

                async fn read(&self, _: Self::Subscription) -> Result<Self::Stream> {
                    let items: Vec<Result<Message<CountingAcker, NoCursor, WirePayload>>> =
                        self.events.clone().into_iter().map(Ok).collect();
                    Ok(Box::pin(stream::iter(items)))
                }
            }
            TypedVecReader {
                events: vec![Message::new(
                    Event::create(
                        "acme",
                        "/users",
                        "user.updated",
                        WirePayload::from_string("bad"),
                    )
                    .unwrap(),
                    acker.clone(),
                    NoCursor,
                )],
            }
        };

        let reader =
            DecodeReader::<_, _, UserUpdated>::from_payload_codec(raw, crate::JsonPayloadCodec);
        let mut stream = reader.read(TestSub).await.unwrap();
        let err = stream.next().await.unwrap().unwrap_err();

        assert!(matches!(
            err,
            Error::InvalidPayload(_) | Error::Serialization(_)
        ));
        assert_eq!(
            acker.nack_count.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }

    #[tokio::test]
    async fn event_codec_reader_decodes_domain_enum_pipeline() {
        let raw = VecPayloadReader::new(vec![
            Event::create(
                "acme",
                "/users",
                "user.deleted",
                WirePayload::from_json(&UserDeleted {
                    user_id: "u-1".to_owned(),
                })
                .unwrap(),
            )
            .unwrap(),
        ]);

        let reader = DecodeReader::<_, _, DomainEvent>::new(raw, DomainEventCodec);
        let mut stream = reader.read(TestSub).await.unwrap();
        let msg = stream.next().await.unwrap().unwrap();

        assert_eq!(
            msg.event().payload(),
            &DomainEvent::UserDeleted(UserDeleted {
                user_id: "u-1".to_owned()
            })
        );
    }

    #[tokio::test]
    async fn writer_typed_ext_encodes_with_payload_codec() {
        let inner = CapturingPayloadWriter::default();
        let captured = Arc::clone(&inner.events);
        let writer = inner.encode::<UserUpdated, _>(crate::JsonPayloadCodec);

        writer
            .write(
                &Event::create(
                    "acme",
                    "/users",
                    "user.updated",
                    UserUpdated {
                        user_id: "u-1".to_owned(),
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let raw = captured.lock().unwrap();
        let decoded: UserUpdated = raw[0].payload().to_json().unwrap();
        assert_eq!(decoded.user_id, "u-1");
    }

    #[tokio::test]
    async fn reader_typed_ext_decodes_with_payload_codec() {
        let raw = VecPayloadReader::new(vec![
            Event::create(
                "acme",
                "/users",
                "user.updated",
                WirePayload::from_json(&UserUpdated {
                    user_id: "u-1".to_owned(),
                })
                .unwrap(),
            )
            .unwrap(),
        ]);

        let reader = raw.decode::<UserUpdated, _>(crate::JsonPayloadCodec);
        let mut stream = reader.read(TestSub).await.unwrap();
        let msg = stream.next().await.unwrap().unwrap();

        assert_eq!(msg.event().payload().user_id, "u-1");
    }

    #[tokio::test]
    async fn typed_writer_encodes_to_payload_writer() {
        let inner = CapturingPayloadWriter::default();
        let captured = Arc::clone(&inner.events);
        let writer = inner.encode::<UserUpdated, _>(crate::JsonPayloadCodec);

        writer
            .write(
                &Event::create(
                    "acme",
                    "/users",
                    "user.updated",
                    UserUpdated {
                        user_id: "u-1".to_owned(),
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let raw = captured.lock().unwrap();
        let decoded: UserUpdated = raw[0].payload().to_json().unwrap();
        assert_eq!(decoded.user_id, "u-1");
    }
}
