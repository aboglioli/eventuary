use std::pin::Pin;

use futures::{Stream, StreamExt};

use crate::error::Result;
use crate::io::start_from::{StartFrom, StartableSubscription};
use crate::io::{Cursor, CursorCodec, EncodedCursor, Message, Reader};

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
    start: StartFrom<EncodedCursor>,
}

impl<S> EncodedCursorSubscription<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            start: StartFrom::Latest,
        }
    }

    pub fn inner(&self) -> &S {
        &self.inner
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
        self.start = start;
        self
    }
}

impl<R, Codec, C> Reader for EncodedCursorReader<R, Codec>
where
    R: Reader<Cursor = C> + Send + Sync + 'static,
    R::Subscription: StartableSubscription<C> + Clone + Send + 'static,
    R::Acker: Send + Sync + 'static,
    R::Stream: Send + 'static,
    C: Cursor + Clone + Send + Sync + 'static,
    Codec: CursorCodec<C>,
{
    type Subscription = EncodedCursorSubscription<R::Subscription>;
    type Acker = R::Acker;
    type Cursor = EncodedCursor;
    type Stream = Pin<Box<dyn Stream<Item = Result<Message<Self::Acker, EncodedCursor>>> + Send>>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let inner_subscription = match subscription.start {
            StartFrom::Earliest => subscription.inner.with_start(StartFrom::Earliest),
            StartFrom::Latest => subscription.inner.with_start(StartFrom::Latest),
            StartFrom::Timestamp(ts) => subscription.inner.with_start(StartFrom::Timestamp(ts)),
            StartFrom::After(encoded) => {
                let decoded = self.codec.decode(&encoded)?;
                subscription.inner.with_start(StartFrom::After(decoded))
            }
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
    use crate::io::acker::NoopAcker;
    use crate::io::{CursorId, CursorOrder, JsonCursorCodec};
    use crate::payload::Payload;

    #[derive(
        Debug, Clone, Eq, PartialEq, Ord, PartialOrd, serde::Serialize, serde::Deserialize,
    )]
    struct TestCursor(i64);

    impl Cursor for TestCursor {
        fn order_key(&self) -> CursorOrder {
            CursorOrder::from_i64(self.0)
        }
    }

    #[derive(Debug, Clone, Default)]
    struct TestSubscription {
        start: StartFrom<TestCursor>,
    }

    impl StartableSubscription<TestCursor> for TestSubscription {
        fn with_start(mut self, start: StartFrom<TestCursor>) -> Self {
            self.start = start;
            self
        }
    }

    struct VecReader {
        seen_start: Mutex<Option<StartFrom<TestCursor>>>,
    }

    impl VecReader {
        fn new() -> Self {
            Self {
                seen_start: Mutex::new(None),
            }
        }
    }

    impl Reader for VecReader {
        type Subscription = TestSubscription;
        type Acker = NoopAcker;
        type Cursor = TestCursor;
        type Stream = Pin<Box<dyn Stream<Item = Result<Message<NoopAcker, TestCursor>>> + Send>>;

        async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
            *self.seen_start.lock().unwrap() = Some(subscription.start);
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
    async fn decodes_start_after_cursor_for_inner_subscription() {
        let inner = VecReader::new();
        let codec = JsonCursorCodec::<TestCursor>::new("eventuary.test.cursor.v1").unwrap();
        let encoded = codec.encode(&TestCursor(7)).unwrap();
        let reader = EncodedCursorReader::new(inner, codec);
        let subscription = EncodedCursorSubscription::new(TestSubscription::default())
            .with_start(StartFrom::After(encoded));

        let mut stream = reader.read(subscription).await.unwrap();
        let _ = stream.next().await.unwrap().unwrap();

        let seen = reader.inner().seen_start.lock().unwrap().clone();
        assert_eq!(seen, Some(StartFrom::After(TestCursor(7))));
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

        let seen = reader.inner().seen_start.lock().unwrap().clone();
        assert_eq!(seen, Some(StartFrom::Timestamp(ts)));
    }
}
