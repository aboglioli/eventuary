use std::num::NonZeroU32;
use std::time::Duration;

use futures::StreamExt;
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::error::Result;
use crate::io::Reader;
use crate::io::stream::SpawnedStream;

#[derive(Debug, Clone, Copy)]
pub enum RateLimit {
    MessagesPerSec(NonZeroU32),
}

pub struct RateLimitReader<R> {
    inner: R,
    interval: Duration,
}

impl<R> RateLimitReader<R> {
    pub fn new(inner: R, rate: RateLimit) -> Self {
        let interval = match rate {
            RateLimit::MessagesPerSec(n) => Duration::from_secs_f64(1.0 / n.get() as f64),
        };
        Self { inner, interval }
    }
}

impl<R> Reader for RateLimitReader<R>
where
    R: Reader + Send + Sync + 'static,
    R::Subscription: Send + 'static,
    R::Acker: Send + Sync + 'static,
    R::Cursor: Send + Sync + 'static,
    R::Stream: Send + 'static,
{
    type Subscription = R::Subscription;
    type Acker = R::Acker;
    type Cursor = R::Cursor;
    type Stream = SpawnedStream<R::Acker, R::Cursor>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let inner = self.inner.read(subscription).await?;
        let interval = self.interval;
        let (tx, rx) = mpsc::channel(64);

        let handle = tokio::spawn(async move {
            let mut inner = Box::pin(inner);
            let mut next_allowed: Option<Instant> = None;
            while let Some(item) = inner.next().await {
                let is_ok = item.is_ok();
                if is_ok && let Some(deadline) = next_allowed {
                    tokio::time::sleep_until(deadline).await;
                }
                if tx.send(item).await.is_err() {
                    return;
                }
                if is_ok {
                    next_allowed = Some(Instant::now() + interval);
                }
            }
        });

        Ok(SpawnedStream::new(rx, handle))
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
    use crate::io::Message;
    use crate::io::acker::NoopAcker;
    use crate::payload::Payload;

    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    struct TestCursor(u64);

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
            Ok(Box::pin(stream::iter(events.into_iter().enumerate().map(
                |(i, e)| Ok(Message::new(e, NoopAcker, TestCursor(i as u64))),
            ))))
        }
    }

    fn ev() -> Event {
        Event::create("org", "/x", "thing.happened", Payload::from_string("p")).unwrap()
    }

    #[tokio::test]
    async fn rate_limits_delivery() {
        let events: Vec<Event> = (0..5).map(|_| ev()).collect();
        let reader = VecReader {
            events: Mutex::new(Some(events)),
        };
        let rate = RateLimit::MessagesPerSec(NonZeroU32::new(100).unwrap());
        let limited = RateLimitReader::new(reader, rate);
        let mut stream = limited.read(()).await.unwrap();

        let start = Instant::now();
        for _ in 0..5 {
            let msg = tokio::time::timeout(Duration::from_secs(2), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            msg.ack().await.unwrap();
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(35),
            "rate limit too fast: {elapsed:?}"
        );
    }
}
