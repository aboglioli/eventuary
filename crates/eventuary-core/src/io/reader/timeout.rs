use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Stream;
use tokio::sync::Notify;

use crate::error::Result;
use crate::io::{Acker, Message, Reader};

pub struct TimeoutReader<R> {
    inner: R,
    timeout: Duration,
}

impl<R> TimeoutReader<R> {
    pub fn new(inner: R, timeout: Duration) -> Self {
        Self { inner, timeout }
    }
}

impl<R> Reader for TimeoutReader<R>
where
    R: Reader + Send + Sync + 'static,
    R::Subscription: Send + 'static,
    R::Acker: Clone + Send + Sync + 'static,
    R::Cursor: Send + Sync + 'static,
    R::Stream: 'static,
{
    type Subscription = R::Subscription;
    type Acker = TimeoutAcker<R::Acker>;
    type Cursor = R::Cursor;
    type Stream = TimeoutStream<R>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let inner = self.inner.read(subscription).await?;
        Ok(TimeoutStream {
            inner: Box::pin(inner),
            timeout: self.timeout,
        })
    }
}

pub struct TimeoutStream<R: Reader> {
    inner: Pin<Box<R::Stream>>,
    timeout: Duration,
}

impl<R> Stream for TimeoutStream<R>
where
    R: Reader,
    R::Acker: Clone + Send + Sync + 'static,
{
    type Item = Result<Message<TimeoutAcker<R::Acker>, R::Cursor>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(msg))) => {
                let (event, inner_acker, cursor) = msg.into_parts();
                let notify = Arc::new(Notify::new());
                let resolved = Arc::new(AtomicBool::new(false));
                let acker = TimeoutAcker {
                    inner: inner_acker,
                    notify: Arc::clone(&notify),
                    resolved: Arc::clone(&resolved),
                };
                let acker_for_timer = acker.clone();
                let timeout = self.timeout;
                tokio::spawn(async move {
                    tokio::select! {
                        _ = tokio::time::sleep(timeout) => {
                            if acker_for_timer
                                .resolved
                                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                                .is_ok()
                            {
                                let _ = acker_for_timer.inner.nack().await;
                            }
                        }
                        _ = notify.notified() => {}
                    }
                });
                Poll::Ready(Some(Ok(Message::new(event, acker, cursor))))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Clone)]
pub struct TimeoutAcker<A: Acker> {
    inner: A,
    notify: Arc<Notify>,
    resolved: Arc<AtomicBool>,
}

impl<A: Acker> Acker for TimeoutAcker<A> {
    async fn ack(&self) -> Result<()> {
        if self
            .resolved
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Ok(());
        }
        self.notify.notify_waiters();
        self.inner.ack().await
    }

    async fn nack(&self) -> Result<()> {
        if self
            .resolved
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Ok(());
        }
        self.notify.notify_waiters();
        self.inner.nack().await
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
    use crate::io::{Cursor, Reader};
    use crate::payload::Payload;

    type TestItems = Mutex<Option<Vec<Result<Message<NoopAcker, TestCursor>>>>>;

    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    struct TestCursor(u64);

    impl Cursor for TestCursor {}

    struct VecReader {
        items: TestItems,
    }

    impl Reader for VecReader {
        type Subscription = ();
        type Acker = NoopAcker;
        type Cursor = TestCursor;
        type Stream = Pin<Box<dyn Stream<Item = Result<Message<NoopAcker, TestCursor>>> + Send>>;

        async fn read(&self, _: ()) -> Result<Self::Stream> {
            let items = self.items.lock().unwrap().take().unwrap_or_default();
            Ok(Box::pin(stream::iter(items)))
        }
    }

    fn ev() -> Event {
        Event::create("org", "/x", "thing.happened", Payload::from_string("p")).unwrap()
    }

    #[tokio::test]
    async fn timeout_reader_forwards_immediate_ack() {
        let reader = VecReader {
            items: Mutex::new(Some(vec![Ok(Message::new(ev(), NoopAcker, TestCursor(1)))])),
        };
        let timed = TimeoutReader::new(reader, Duration::from_secs(60));
        let mut stream = timed.read(()).await.unwrap();
        let msg = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        msg.ack().await.unwrap();
    }

    #[tokio::test]
    async fn timeout_reader_does_not_block_on_delivery() {
        let reader = VecReader {
            items: Mutex::new(Some(vec![Ok(Message::new(ev(), NoopAcker, TestCursor(1)))])),
        };
        let timed = TimeoutReader::new(reader, Duration::from_millis(50));
        let mut stream = timed.read(()).await.unwrap();
        let msg = tokio::time::timeout(Duration::from_millis(200), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(msg.event().topic().as_str(), "thing.happened");
        msg.ack().await.unwrap();
    }

    #[tokio::test]
    async fn timeout_reader_allows_ack_before_timeout() {
        let reader = VecReader {
            items: Mutex::new(Some(vec![Ok(Message::new(ev(), NoopAcker, TestCursor(1)))])),
        };
        let timed = TimeoutReader::new(reader, Duration::from_secs(60));
        let mut stream = timed.read(()).await.unwrap();
        let msg = stream.next().await.unwrap().unwrap();
        msg.ack().await.unwrap();
    }

    #[derive(Clone, Default)]
    struct NackCounter {
        nacks: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Acker for NackCounter {
        async fn ack(&self) -> Result<()> {
            Ok(())
        }
        async fn nack(&self) -> Result<()> {
            self.nacks.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    type CountingItems = Mutex<Option<Vec<Result<Message<NackCounter, TestCursor>>>>>;

    struct CountingReader {
        items: CountingItems,
    }

    impl Reader for CountingReader {
        type Subscription = ();
        type Acker = NackCounter;
        type Cursor = TestCursor;
        type Stream = Pin<Box<dyn Stream<Item = Result<Message<NackCounter, TestCursor>>> + Send>>;

        async fn read(&self, _: ()) -> Result<Self::Stream> {
            let items = self.items.lock().unwrap().take().unwrap_or_default();
            Ok(Box::pin(stream::iter(items)))
        }
    }

    #[tokio::test]
    async fn timeout_reader_nacks_inner_on_expiry() {
        let counter = NackCounter::default();
        let reader = CountingReader {
            items: Mutex::new(Some(vec![Ok(Message::new(
                ev(),
                counter.clone(),
                TestCursor(1),
            ))])),
        };
        let timed = TimeoutReader::new(reader, Duration::from_millis(20));
        let mut stream = timed.read(()).await.unwrap();
        let _msg = stream.next().await.unwrap().unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(counter.nacks.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}
