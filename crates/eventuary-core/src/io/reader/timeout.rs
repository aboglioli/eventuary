use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_util::time::DelayQueue;
use tokio_util::time::delay_queue::Key as DelayKey;

use crate::error::Result;
use crate::io::stream::SpawnedStream;
use crate::io::{Acker, Message, Reader};

pub type TimeoutStream<A, C> = SpawnedStream<TimeoutAcker<A>, C>;

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
    R::Acker: Send + Sync + 'static,
    R::Cursor: Send + Sync + 'static,
    R::Stream: Send + 'static,
{
    type Subscription = R::Subscription;
    type Acker = TimeoutAcker<R::Acker>;
    type Cursor = R::Cursor;
    type Stream = TimeoutStream<R::Acker, R::Cursor>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let inner = self.inner.read(subscription).await?;
        let timeout = self.timeout;
        let (tx_out, rx_out) = mpsc::channel(64);
        let (tx_resolve, mut rx_resolve) = mpsc::channel::<u64>(64);

        let handle = tokio::spawn(async move {
            let mut inner = Box::pin(inner);
            let mut delays: DelayQueue<u64> = DelayQueue::new();
            let mut pending: HashMap<u64, PendingTimer<R::Acker>> = HashMap::new();
            let mut next_id: u64 = 0;
            let mut inner_done = false;

            loop {
                tokio::select! {
                    biased;
                    Some(id) = rx_resolve.recv() => {
                        if let Some(entry) = pending.remove(&id) {
                            delays.remove(&entry.delay_key);
                        }
                    }
                    Some(expired) = delays.next() => {
                        let id = expired.into_inner();
                        if let Some(entry) = pending.remove(&id)
                            && !entry.shared.resolved.swap(true, Ordering::SeqCst)
                        {
                            let _ = entry.shared.inner.nack().await;
                        }
                    }
                    item = inner.next(), if !inner_done => {
                        match item {
                            Some(Ok(msg)) => {
                                let id = next_id;
                                next_id = next_id.wrapping_add(1);
                                let (event, inner_acker, cursor) = msg.into_parts();
                                let shared = Arc::new(TimeoutShared {
                                    inner: inner_acker,
                                    resolved: AtomicBool::new(false),
                                });
                                let delay_key = delays.insert(id, timeout);
                                pending.insert(
                                    id,
                                    PendingTimer {
                                        delay_key,
                                        shared: Arc::clone(&shared),
                                    },
                                );
                                let acker = TimeoutAcker {
                                    shared,
                                    tx_resolve: tx_resolve.clone(),
                                    id,
                                };
                                if tx_out.send(Ok(Message::new(event, acker, cursor))).await.is_err() {
                                    return;
                                }
                            }
                            Some(Err(e)) => {
                                let _ = tx_out.send(Err(e)).await;
                                return;
                            }
                            None => {
                                inner_done = true;
                                if pending.is_empty() {
                                    return;
                                }
                            }
                        }
                    }
                    else => {
                        if inner_done && pending.is_empty() {
                            return;
                        }
                    }
                }
            }
        });

        Ok(SpawnedStream::new(rx_out, handle))
    }
}

struct PendingTimer<A: Acker> {
    delay_key: DelayKey,
    shared: Arc<TimeoutShared<A>>,
}

struct TimeoutShared<A: Acker> {
    inner: A,
    resolved: AtomicBool,
}

pub struct TimeoutAcker<A: Acker> {
    shared: Arc<TimeoutShared<A>>,
    tx_resolve: mpsc::Sender<u64>,
    id: u64,
}

impl<A: Acker> Acker for TimeoutAcker<A> {
    async fn ack(&self) -> Result<()> {
        if self.shared.resolved.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let _ = self.tx_resolve.send(self.id).await;
        self.shared.inner.ack().await
    }

    async fn nack(&self) -> Result<()> {
        if self.shared.resolved.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let _ = self.tx_resolve.send(self.id).await;
        self.shared.inner.nack().await
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::Mutex;

    use futures::{Stream, stream};

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
        use futures::StreamExt;
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
        use futures::StreamExt;
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

    #[derive(Clone, Default)]
    struct NackCounter {
        nacks: Arc<std::sync::atomic::AtomicUsize>,
        acks: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Acker for NackCounter {
        async fn ack(&self) -> Result<()> {
            self.acks.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn nack(&self) -> Result<()> {
            self.nacks.fetch_add(1, Ordering::SeqCst);
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
        use futures::StreamExt;
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
        assert_eq!(counter.nacks.load(Ordering::SeqCst), 1);
        assert_eq!(counter.acks.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn timeout_reader_skips_inner_nack_after_ack() {
        use futures::StreamExt;
        let counter = NackCounter::default();
        let reader = CountingReader {
            items: Mutex::new(Some(vec![Ok(Message::new(
                ev(),
                counter.clone(),
                TestCursor(1),
            ))])),
        };
        let timed = TimeoutReader::new(reader, Duration::from_millis(50));
        let mut stream = timed.read(()).await.unwrap();
        let msg = stream.next().await.unwrap().unwrap();
        msg.ack().await.unwrap();
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert_eq!(counter.acks.load(Ordering::SeqCst), 1);
        assert_eq!(counter.nacks.load(Ordering::SeqCst), 0);
    }
}
