use std::num::NonZeroUsize;
use std::time::Duration;

use futures::StreamExt;
use tokio::sync::mpsc;

use crate::error::Result;
use crate::event::Event;
use crate::io::Reader;
use crate::io::reader::batch::{BatchAcker, BatchCursor, flush_batch};
use crate::io::stream::SpawnedStream;

pub struct WindowReader<R> {
    inner: R,
    max_size: NonZeroUsize,
    max_wait: Duration,
}

impl<R> WindowReader<R> {
    pub fn new(inner: R, max_size: NonZeroUsize, max_wait: Duration) -> Self {
        Self {
            inner,
            max_size,
            max_wait,
        }
    }
}

impl<R> Reader for WindowReader<R>
where
    R: Reader + Send + Sync + 'static,
    R::Subscription: Send + 'static,
    R::Acker: 'static,
    R::Cursor: Clone + Send + Sync + 'static,
    R::Stream: Send + 'static,
{
    type Subscription = R::Subscription;
    type Acker = BatchAcker<R::Acker>;
    type Cursor = BatchCursor<R::Cursor>;
    type Stream = SpawnedStream<BatchAcker<R::Acker>, BatchCursor<R::Cursor>>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let inner = self.inner.read(subscription).await?;
        let max_size = self.max_size.get();
        let max_wait = self.max_wait;
        let (tx, rx) = mpsc::channel(64);

        let handle = tokio::spawn(async move {
            let mut inner = Box::pin(inner);
            let mut buffer: Vec<(Event, R::Acker, R::Cursor)> = Vec::with_capacity(max_size);
            let mut flush_at: Option<tokio::time::Instant> = None;

            loop {
                let sleep = flush_at
                    .map(tokio::time::sleep_until)
                    .unwrap_or_else(|| tokio::time::sleep(Duration::from_secs(3600)));

                tokio::select! {
                    item = inner.next() => {
                        match item {
                            Some(Ok(msg)) => {
                                let is_first = buffer.is_empty();
                                let (event, acker, cursor) = msg.into_parts();
                                buffer.push((event, acker, cursor));
                                if is_first {
                                    flush_at = Some(tokio::time::Instant::now() + max_wait);
                                }
                                if buffer.len() >= max_size {
                                    if !flush_batch(&mut buffer, &tx).await {
                                        return;
                                    }
                                    flush_at = None;
                                }
                            }
                            Some(Err(e)) => {
                                flush_batch(&mut buffer, &tx).await;
                                let _ = tx.send(Err(e)).await;
                                return;
                            }
                            None => {
                                flush_batch(&mut buffer, &tx).await;
                                return;
                            }
                        }
                    }
                    _ = sleep => {
                        if !buffer.is_empty() {
                            if !flush_batch(&mut buffer, &tx).await {
                                return;
                            }
                            flush_at = None;
                        }
                    }
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
    use crate::io::Cursor;
    use crate::io::Message;
    use crate::io::acker::NoopAcker;
    use crate::payload::Payload;

    #[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
    struct TestCursor(i64);

    impl Cursor for TestCursor {}

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
            let iter = events
                .into_iter()
                .enumerate()
                .map(|(i, e)| Ok(Message::new(e, NoopAcker, TestCursor(i as i64 + 1))));
            Ok(Box::pin(stream::iter(iter)))
        }
    }

    fn ev(key: &str) -> Event {
        Event::builder("acme", "/x", "thing.happened", Payload::from_string("p"))
            .unwrap()
            .key(key)
            .unwrap()
            .build()
            .expect("valid event")
    }

    #[tokio::test]
    async fn window_reader_flushes_on_timeout() {
        let events: Vec<Event> = vec![ev("k0"), ev("k1")];
        let reader = VecReader {
            events: Mutex::new(Some(events)),
        };
        let windowed = WindowReader::new(
            reader,
            NonZeroUsize::new(10).unwrap(),
            Duration::from_millis(50),
        );
        let mut stream = windowed.read(()).await.unwrap();
        for key in ["k0", "k1"] {
            let msg = tokio::time::timeout(Duration::from_secs(2), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(msg.event().key().unwrap().as_str(), key);
            msg.ack().await.unwrap();
        }
        let end = tokio::time::timeout(Duration::from_secs(1), stream.next()).await;
        assert!(matches!(end, Ok(None)));
    }

    #[tokio::test]
    async fn window_reader_also_flushes_on_max_size() {
        let events: Vec<Event> = (0..5).map(|i| ev(&format!("k{i}"))).collect();
        let reader = VecReader {
            events: Mutex::new(Some(events)),
        };
        let windowed = WindowReader::new(
            reader,
            NonZeroUsize::new(3).unwrap(),
            Duration::from_secs(10),
        );
        let mut stream = windowed.read(()).await.unwrap();
        for i in 0..5 {
            let msg = tokio::time::timeout(Duration::from_secs(2), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(msg.event().key().unwrap().as_str(), &format!("k{i}"));
            msg.ack().await.unwrap();
        }
        let end = tokio::time::timeout(Duration::from_secs(1), stream.next()).await;
        assert!(matches!(end, Ok(None)));
    }
}
