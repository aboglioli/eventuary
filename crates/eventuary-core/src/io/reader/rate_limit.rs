use std::num::NonZeroU32;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Stream;
use tokio::time::Interval;

use crate::error::Result;
use crate::io::{Message, Reader};

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
            RateLimit::MessagesPerSec(n) => {
                Duration::from_secs_f64(1.0 / n.get() as f64)
            }
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
    R::Stream: 'static,
{
    type Subscription = R::Subscription;
    type Acker = R::Acker;
    type Cursor = R::Cursor;
    type Stream = RateLimitedStream<R>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let inner = self.inner.read(subscription).await?;
        Ok(RateLimitedStream {
            inner: Box::pin(inner),
            interval: tokio::time::interval(self.interval),
        })
    }
}

pub struct RateLimitedStream<R: Reader> {
    inner: Pin<Box<R::Stream>>,
    interval: Interval,
}

impl<R> Stream for RateLimitedStream<R>
where
    R: Reader + Send + Sync + 'static,
    R::Acker: Send + Sync + 'static,
    R::Cursor: Send + Sync + 'static,
{
    type Item = Result<Message<R::Acker, R::Cursor>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.interval.poll_tick(cx).is_pending() {
            return Poll::Pending;
        }
        self.inner.as_mut().poll_next(cx)
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
    async fn rate_limits_delivery() {
        let events: Vec<Event> = (0..5).map(|_| ev()).collect();
        let reader = VecReader {
            events: Mutex::new(Some(events)),
        };
        let rate = RateLimit::MessagesPerSec(NonZeroU32::new(100).unwrap()); // 10ms interval
        let limited = RateLimitReader::new(reader, rate);
        let mut stream = limited.read(()).await.unwrap();

        let start = tokio::time::Instant::now();
        for _ in 0..5 {
            let msg = tokio::time::timeout(Duration::from_secs(2), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            msg.ack().await.unwrap();
        }
        let elapsed = start.elapsed();
        // 5 messages at 100/s = 4 intervals of 10ms = at least 40ms
        assert!(
            elapsed >= Duration::from_millis(35),
            "rate limit too fast: {elapsed:?}"
        );
    }
}
