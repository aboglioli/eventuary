use std::pin::Pin;

use futures::{Stream, StreamExt};

use crate::error::Result;
use crate::io::cursor::{CursorCodec, EncodedCursor};
use crate::io::position::{StartFrom, StartableSubscription};
use crate::io::{Cursor, Message, Reader};

pub struct EncodedCursorReader<R, Codec> {
    inner: R,
    codec: Codec,
}

impl<R, Codec> EncodedCursorReader<R, Codec> {
    pub fn new(inner: R, codec: Codec) -> Self {
        Self { inner, codec }
    }

    pub fn inner(&self) -> &R {
        &self.inner
    }

    pub fn codec(&self) -> &Codec {
        &self.codec
    }
}

#[derive(Debug, Clone)]
pub struct EncodedCursorSubscription<S> {
    inner: S,
    starts: Vec<StartFrom<EncodedCursor>>,
}

impl<S> EncodedCursorSubscription<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            starts: Vec::new(),
        }
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }

    pub fn starts(&self) -> &[StartFrom<EncodedCursor>] {
        &self.starts
    }

    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S> StartableSubscription<EncodedCursor> for EncodedCursorSubscription<S>
where
    S: Clone + Send + 'static,
{
    fn with_start(mut self, start: StartFrom<EncodedCursor>) -> Self {
        self.starts = vec![start];
        self
    }

    fn with_starts(mut self, starts: Vec<StartFrom<EncodedCursor>>) -> Self {
        self.starts = starts;
        self
    }
}

impl<R, Codec, C, P> Reader<P> for EncodedCursorReader<R, Codec>
where
    R: Reader<P, Cursor = C> + Send + Sync + 'static,
    R::Subscription: StartableSubscription<C> + Clone + Send + 'static,
    R::Acker: Send + Sync + 'static,
    R::Stream: Send + 'static,
    C: Cursor + Clone + Ord + Send + Sync + 'static,
    Codec: CursorCodec<C>,
    P: Send + 'static,
{
    type Subscription = EncodedCursorSubscription<R::Subscription>;
    type Acker = R::Acker;
    type Cursor = EncodedCursor;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Message<Self::Acker, EncodedCursor, P>>> + Send>>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let mut decoded_starts = Vec::with_capacity(subscription.starts.len());
        for start in subscription.starts {
            let decoded = match start {
                StartFrom::Earliest => StartFrom::Earliest,
                StartFrom::Latest => StartFrom::Latest,
                StartFrom::Timestamp(timestamp) => StartFrom::Timestamp(timestamp),
                StartFrom::After(encoded) => StartFrom::After(self.codec.decode(&encoded)?),
            };
            decoded_starts.push(decoded);
        }

        let inner_subscription = if decoded_starts.is_empty() {
            subscription.inner
        } else {
            subscription.inner.with_starts(decoded_starts)
        };

        let inner_stream = self.inner.read(inner_subscription).await?;
        let codec = self.codec.clone();
        Ok(Box::pin(inner_stream.map(move |result| {
            let message = result?;
            let (event, acker, cursor) = message.into_parts();
            let encoded = codec.encode(&cursor)?;
            Ok(Message::new(event, acker, encoded))
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;

    use futures::stream;

    use crate::event::Event;
    use crate::io::CursorId;
    use crate::io::acker::NoopAcker;
    use crate::io::cursor::{CursorOrder, JsonCursorCodec};
    use crate::payload::Payload;

    #[derive(
        Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, serde::Serialize, serde::Deserialize,
    )]
    struct TestCursor(i64);

    impl Cursor for TestCursor {
        fn order_key(&self) -> CursorOrder {
            CursorOrder::from_i64(self.0)
        }
    }

    #[derive(Debug, Clone, Default, Eq, PartialEq)]
    struct TestSubscription {
        starts: Vec<StartFrom<TestCursor>>,
    }

    impl StartableSubscription<TestCursor> for TestSubscription {
        fn with_start(mut self, start: StartFrom<TestCursor>) -> Self {
            self.starts = vec![start];
            self
        }

        fn with_starts(mut self, starts: Vec<StartFrom<TestCursor>>) -> Self {
            self.starts = starts;
            self
        }
    }

    struct VecReader {
        seen_starts: Mutex<Option<Vec<StartFrom<TestCursor>>>>,
    }

    impl VecReader {
        fn new() -> Self {
            Self {
                seen_starts: Mutex::new(None),
            }
        }
    }

    impl Reader for VecReader {
        type Subscription = TestSubscription;
        type Acker = NoopAcker;
        type Cursor = TestCursor;
        type Stream = Pin<Box<dyn Stream<Item = Result<Message<NoopAcker, TestCursor>>> + Send>>;

        async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
            *self.seen_starts.lock().unwrap() = Some(subscription.starts);
            let event = Event::builder("acme", "/x", "thing.happened", Payload::from_string("p"))
                .unwrap()
                .build()
                .unwrap();
            Ok(Box::pin(stream::once(async move {
                Ok(Message::new(event, NoopAcker, TestCursor(42)))
            })))
        }
    }

    #[tokio::test]
    async fn encodes_delivered_cursor() {
        let reader = EncodedCursorReader::new(
            VecReader::new(),
            JsonCursorCodec::<TestCursor>::new("eventuary.test.cursor.v1").unwrap(),
        );
        let mut stream = reader
            .read(EncodedCursorSubscription::new(TestSubscription::default()))
            .await
            .unwrap();
        let message = stream.next().await.unwrap().unwrap();
        assert_eq!(message.cursor().id_ref(), &CursorId::global());
        assert_eq!(message.cursor().kind().as_str(), "eventuary.test.cursor.v1");
        assert_eq!(message.cursor().order(), &CursorOrder::from_i64(42));
    }

    #[tokio::test]
    async fn empty_starts_passes_inner_subscription_unchanged() {
        let reader = EncodedCursorReader::new(
            VecReader::new(),
            JsonCursorCodec::<TestCursor>::new("eventuary.test.cursor.v1").unwrap(),
        );
        let mut stream = reader
            .read(EncodedCursorSubscription::new(TestSubscription::default()))
            .await
            .unwrap();
        let _ = stream.next().await.unwrap().unwrap();

        let seen = reader.inner().seen_starts.lock().unwrap().clone();
        assert_eq!(seen, Some(vec![]));
    }

    #[tokio::test]
    async fn decodes_all_start_after_cursors_for_inner_subscription() {
        let inner = VecReader::new();
        let codec = JsonCursorCodec::<TestCursor>::new("eventuary.test.cursor.v1").unwrap();
        let first = codec.encode(&TestCursor(7)).unwrap();
        let second = codec.encode(&TestCursor(9)).unwrap();
        let reader = EncodedCursorReader::new(inner, codec);
        let subscription = EncodedCursorSubscription::new(TestSubscription::default())
            .with_starts(vec![StartFrom::After(first), StartFrom::After(second)]);

        let mut stream = reader.read(subscription).await.unwrap();
        let _ = stream.next().await.unwrap().unwrap();

        let seen = reader.inner().seen_starts.lock().unwrap().clone();
        assert_eq!(
            seen,
            Some(vec![
                StartFrom::After(TestCursor(7)),
                StartFrom::After(TestCursor(9)),
            ])
        );
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TypedEventPayload {
        value: String,
    }

    struct TypedVecReader {
        seen_starts: Mutex<Option<Vec<StartFrom<TestCursor>>>>,
    }

    impl TypedVecReader {
        fn new() -> Self {
            Self {
                seen_starts: Mutex::new(None),
            }
        }
    }

    impl Reader<TypedEventPayload> for TypedVecReader {
        type Subscription = TestSubscription;
        type Acker = NoopAcker;
        type Cursor = TestCursor;
        type Stream = Pin<
            Box<
                dyn Stream<Item = Result<Message<NoopAcker, TestCursor, TypedEventPayload>>> + Send,
            >,
        >;

        async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
            *self.seen_starts.lock().unwrap() = Some(subscription.starts);
            let event = Event::builder(
                "acme",
                "/typed",
                "typed.encoded_cursor",
                TypedEventPayload {
                    value: "typed-cursor".to_owned(),
                },
            )
            .unwrap()
            .build()
            .unwrap();
            Ok(Box::pin(stream::once(async move {
                Ok(Message::new(event, NoopAcker, TestCursor(42)))
            })))
        }
    }

    #[tokio::test]
    async fn encoded_cursor_reader_preserves_typed_payloads() {
        let codec = JsonCursorCodec::<TestCursor>::new("test.cursor").unwrap();
        let reader = EncodedCursorReader::new(TypedVecReader::new(), codec);
        let sub = EncodedCursorSubscription::new(TestSubscription::default());

        let mut stream = Reader::<TypedEventPayload>::read(&reader, sub)
            .await
            .unwrap();
        let msg = stream.next().await.unwrap().unwrap();

        assert_eq!(msg.event().payload().value, "typed-cursor");
        assert_eq!(msg.cursor().kind().as_str(), "test.cursor");
    }

    #[tokio::test]
    async fn forwards_earliest_latest_timestamp_unchanged() {
        use chrono::{TimeZone, Utc};

        let inner = VecReader::new();
        let codec = JsonCursorCodec::<TestCursor>::new("eventuary.test.cursor.v1").unwrap();
        let reader = EncodedCursorReader::new(inner, codec);
        let ts = Utc.timestamp_opt(1_700_000_000, 0).unwrap();

        let subscription = EncodedCursorSubscription::new(TestSubscription::default())
            .with_start(StartFrom::Timestamp(ts));

        let mut stream = reader.read(subscription).await.unwrap();
        let _ = stream.next().await.unwrap().unwrap();

        let seen = reader.inner().seen_starts.lock().unwrap().clone();
        assert_eq!(seen, Some(vec![StartFrom::Timestamp(ts)]));
    }
}
