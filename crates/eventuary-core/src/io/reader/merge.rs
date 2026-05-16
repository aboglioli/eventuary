use std::pin::Pin;

use either::Either;
use futures::{Stream, StreamExt};

use crate::error::Result;
use crate::io::{Acker, Message, Reader};

pub struct MergeReader<R1, R2> {
    r1: R1,
    r2: R2,
}

impl<R1, R2> MergeReader<R1, R2> {
    pub fn new(r1: R1, r2: R2) -> Self {
        Self { r1, r2 }
    }
}

impl<R1, R2> Reader for MergeReader<R1, R2>
where
    R1: Reader + Send + Sync + 'static,
    R2: Reader + Send + Sync + 'static,
    R1::Subscription: Send + 'static,
    R2::Subscription: Send + 'static,
    R1::Acker: Send + Sync + 'static,
    R2::Acker: Send + Sync + 'static,
    R1::Cursor: Send + Sync + 'static,
    R2::Cursor: Send + Sync + 'static,
    R1::Stream: Send + 'static,
    R2::Stream: Send + 'static,
{
    type Subscription = (R1::Subscription, R2::Subscription);
    type Acker = EitherAcker<R1::Acker, R2::Acker>;
    type Cursor = Either<R1::Cursor, R2::Cursor>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Message<Self::Acker, Self::Cursor>>> + Send>>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let (sub1, sub2) = subscription;
        let stream1 = self.r1.read(sub1).await?;
        let stream2 = self.r2.read(sub2).await?;

        let s1 = Box::pin(stream1.map(|item| {
            item.map(|msg| {
                let (event, acker, cursor) = msg.into_parts();
                Message::new(
                    event,
                    EitherAcker { inner: Either::Left(acker) },
                    Either::Left(cursor),
                )
            })
        }));

        let s2 = Box::pin(stream2.map(|item| {
            item.map(|msg| {
                let (event, acker, cursor) = msg.into_parts();
                Message::new(
                    event,
                    EitherAcker { inner: Either::Right(acker) },
                    Either::Right(cursor),
                )
            })
        }));

        let merged = futures::stream::select(s1, s2);
        Ok(Box::pin(merged))
    }
}

pub struct EitherAcker<A1: Acker, A2: Acker> {
    inner: Either<A1, A2>,
}

impl<A1: Acker, A2: Acker> Acker for EitherAcker<A1, A2> {
    async fn ack(&self) -> Result<()> {
        match &self.inner {
            Either::Left(a) => a.ack().await,
            Either::Right(a) => a.ack().await,
        }
    }

    async fn nack(&self) -> Result<()> {
        match &self.inner {
            Either::Left(a) => a.nack().await,
            Either::Right(a) => a.nack().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::time::Duration;

    use futures::{Stream, StreamExt, stream};

    use super::*;
    use crate::event::Event;
    use crate::io::acker::NoopAcker;
    use crate::payload::Payload;

    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    struct TestCursor(u64);

    impl crate::io::Cursor for TestCursor {}

    struct VecReader {
        events: Mutex<Option<Vec<Event>>>,
    }

    impl Reader for VecReader {
        type Subscription = ();
        type Acker = NoopAcker;
        type Cursor = TestCursor;
        type Stream = Pin<Box<dyn Stream<Item = Result<Message<NoopAcker, TestCursor>>> + Send>>;

        async fn read(&self, _: ()) -> Result<Self::Stream> {
            let events = self.events.lock().unwrap().take().unwrap_or_default();
            Ok(Box::pin(stream::iter(
                events.into_iter().enumerate().map(|(i, e)| {
                    Ok(Message::new(e, NoopAcker, TestCursor(i as u64)))
                }),
            )))
        }
    }

    fn ev() -> Event {
        Event::create("org", "/x", "thing.happened", Payload::from_string("p")).unwrap()
    }

    #[tokio::test]
    async fn merges_two_readers() {
        let reader1 = VecReader {
            events: Mutex::new(Some(vec![ev(), ev(), ev()])),
        };
        let reader2 = VecReader {
            events: Mutex::new(Some(vec![ev(), ev()])),
        };
        let merged = MergeReader::new(reader1, reader2);
        let mut stream = merged.read(((), ())).await.unwrap();

        let mut count = 0usize;
        for _ in 0..5 {
            let msg = tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            msg.ack().await.unwrap();
            count += 1;
        }
        assert_eq!(count, 5);
        // Stream should be exhausted
        let end = tokio::time::timeout(Duration::from_millis(200), stream.next()).await;
        assert!(end.is_err() || end.unwrap().is_none());
    }
}
