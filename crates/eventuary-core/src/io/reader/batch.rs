use std::vec::Vec;

use futures::StreamExt;
use tokio::sync::mpsc;

use crate::error::Result;
use crate::event::Event;
use crate::io::stream::SpawnedStream;
use crate::io::{Acker, Cursor, CursorId, Message, Reader};

pub struct BatchReader<R> {
    inner: R,
    max_batch: usize,
}

impl<R> BatchReader<R> {
    pub fn new(inner: R, max_batch: usize) -> Self {
        Self { inner, max_batch }
    }
}

impl<R> Reader for BatchReader<R>
where
    R: Reader + Send + Sync + 'static,
    R::Subscription: Send + 'static,
    R::Acker: Clone + Send + Sync + 'static,
    R::Cursor: Clone + Send + Sync + 'static,
    R::Stream: Send + 'static,
{
    type Subscription = R::Subscription;
    type Acker = BatchAcker<R::Acker>;
    type Cursor = BatchCursor<R::Cursor>;
    type Stream = SpawnedStream<BatchAcker<R::Acker>, BatchCursor<R::Cursor>>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let inner = self.inner.read(subscription).await?;
        let max_batch = self.max_batch.max(1);
        let (tx, rx) = mpsc::channel(64);

        let handle = tokio::spawn(async move {
            let mut inner = Box::pin(inner);
            let mut buffer: Vec<(Event, R::Acker, R::Cursor)> = Vec::with_capacity(max_batch);

            async fn flush<A, C>(
                buffer: &mut Vec<(Event, A, C)>,
                tx: &mpsc::Sender<Result<Message<BatchAcker<A>, BatchCursor<C>>>>,
            ) where
                A: Acker + Clone + Send + Sync + 'static,
                C: Clone + Send + Sync + 'static,
            {
                if buffer.is_empty() {
                    return;
                }
                let batch_ackers: Vec<A> = buffer.iter().map(|(_, a, _)| a.clone()).collect();
                let batch_cursors: Vec<C> = buffer.iter().map(|(_, _, c)| c.clone()).collect();
                // Deliver each event with the shared batch acker/cursor
                let acker = BatchAcker(batch_ackers);
                let cursor = BatchCursor(batch_cursors);
                for (event, _, _) in buffer.drain(..) {
                    if tx
                        .send(Ok(Message::new(event, acker.clone(), cursor.clone())))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }

            loop {
                tokio::select! {
                    item = inner.next() => {
                        match item {
                            Some(Ok(msg)) => {
                                let (event, acker, cursor) = msg.into_parts();
                                buffer.push((event, acker, cursor));
                                if buffer.len() >= max_batch {
                                    flush(&mut buffer, &tx).await;
                                    if tx.is_closed() { return; }
                                }
                            }
                            Some(Err(e)) => {
                                // Flush remaining before error
                                flush(&mut buffer, &tx).await;
                                let _ = tx.send(Err(e)).await;
                                return;
                            }
                            None => {
                                flush(&mut buffer, &tx).await;
                                return;
                            }
                        }
                    }
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(3600)) => {
                        // Periodic wake-up for windowed batch (used by WindowReader)
                        unreachable!();
                    }
                }
            }
        });

        Ok(SpawnedStream::new(rx, handle))
    }
}

#[derive(Debug, Clone)]
pub struct BatchAcker<A: Acker>(pub Vec<A>);

impl<A: Acker> Acker for BatchAcker<A> {
    async fn ack(&self) -> Result<()> {
        for a in &self.0 {
            a.ack().await?;
        }
        Ok(())
    }

    async fn nack(&self) -> Result<()> {
        for a in &self.0 {
            a.nack().await?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct BatchCursor<C>(pub Vec<C>);

impl<C: Cursor> Cursor for BatchCursor<C> {
    fn id(&self) -> CursorId {
        self.0
            .first()
            .map(|c| c.id())
            .unwrap_or(CursorId::Global)
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::time::Duration;

    use futures::{Stream, StreamExt, stream};

    use super::*;
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
            let iter = events.into_iter().enumerate().map(|(i, e)| {
                Ok(Message::new(e, NoopAcker, TestCursor(i as i64 + 1)))
            });
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
    async fn batch_reader_groups_messages_by_batch_size() {
        let events: Vec<Event> = (0..5).map(|i| ev(&format!("k{i}"))).collect();
        let reader = VecReader {
            events: Mutex::new(Some(events)),
        };
        let batched = BatchReader::new(reader, 2);
        let mut stream = batched.read(()).await.unwrap();

        let msg = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(msg.event().key().unwrap().as_str(), "k0");
        msg.ack().await.unwrap();

        let msg = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(msg.event().key().unwrap().as_str(), "k1");
        // Acking this also acks k0 (same batch)
        msg.ack().await.unwrap();

        let msg = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(msg.event().key().unwrap().as_str(), "k2");
        msg.ack().await.unwrap();

        let msg = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(msg.event().key().unwrap().as_str(), "k3");
        msg.ack().await.unwrap();

        let msg = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(msg.event().key().unwrap().as_str(), "k4");
        msg.ack().await.unwrap();

        // Stream should end
        let end = tokio::time::timeout(Duration::from_secs(1), stream.next()).await;
        assert!(matches!(end, Ok(None)));
    }

    #[tokio::test]
    async fn batch_cursor_first_cursor_id() {
        let cursor = BatchCursor(vec![TestCursor(10), TestCursor(20)]);
        assert_eq!(cursor.id(), TestCursor(10).id());
    }

    #[tokio::test]
    async fn batch_cursor_empty_global() {
        let cursor: BatchCursor<TestCursor> = BatchCursor(vec![]);
        assert_eq!(cursor.id(), CursorId::Global);
    }
}
