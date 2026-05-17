use std::num::NonZeroUsize;
use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::mpsc;

use crate::error::Result;
use crate::event::Event;
use crate::io::stream::SpawnedStream;
use crate::io::{Acker, Cursor, CursorId, Message, Reader};

pub struct BatchReader<R> {
    inner: R,
    max_batch: NonZeroUsize,
}

impl<R> BatchReader<R> {
    pub fn new(inner: R, max_batch: NonZeroUsize) -> Self {
        Self { inner, max_batch }
    }
}

impl<R> Reader for BatchReader<R>
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
        let max_batch = self.max_batch.get();
        let (tx, rx) = mpsc::channel(64);

        let handle = tokio::spawn(async move {
            let mut inner = Box::pin(inner);
            let mut buffer: Vec<(Event, R::Acker, R::Cursor)> = Vec::with_capacity(max_batch);

            while let Some(item) = inner.next().await {
                match item {
                    Ok(msg) => {
                        let (event, acker, cursor) = msg.into_parts();
                        buffer.push((event, acker, cursor));
                        if buffer.len() >= max_batch && !flush_batch(&mut buffer, &tx).await {
                            return;
                        }
                    }
                    Err(e) => {
                        flush_batch(&mut buffer, &tx).await;
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                }
            }
            flush_batch(&mut buffer, &tx).await;
        });

        Ok(SpawnedStream::new(rx, handle))
    }
}

pub(crate) async fn flush_batch<A, C>(
    buffer: &mut Vec<(Event, A, C)>,
    tx: &mpsc::Sender<Result<Message<BatchAcker<A>, BatchCursor<C>>>>,
) -> bool
where
    A: Acker + 'static,
    C: Clone + Send + Sync + 'static,
{
    if buffer.is_empty() {
        return !tx.is_closed();
    }
    let mut ackers: Vec<A> = Vec::with_capacity(buffer.len());
    let mut cursors: Vec<C> = Vec::with_capacity(buffer.len());
    let mut events: Vec<Event> = Vec::with_capacity(buffer.len());
    for (event, acker, cursor) in buffer.drain(..) {
        events.push(event);
        ackers.push(acker);
        cursors.push(cursor);
    }
    let acker = BatchAcker {
        inner: Arc::new(ackers),
    };
    let cursor = BatchCursor {
        inner: Arc::new(cursors),
    };
    for event in events {
        if tx
            .send(Ok(Message::new(event, acker.clone(), cursor.clone())))
            .await
            .is_err()
        {
            return false;
        }
    }
    true
}

#[derive(Debug)]
pub struct BatchAcker<A> {
    inner: Arc<Vec<A>>,
}

impl<A> Clone for BatchAcker<A> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<A: Acker> BatchAcker<A> {
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub async fn ack_subset(&self, indices: &[usize]) -> Result<()> {
        for &i in indices {
            let Some(a) = self.inner.get(i) else {
                return Err(crate::error::Error::Config(format!(
                    "batch acker: index {i} out of range (len {})",
                    self.inner.len()
                )));
            };
            a.ack().await?;
        }
        Ok(())
    }

    pub async fn nack_subset(&self, indices: &[usize]) -> Result<()> {
        for &i in indices {
            let Some(a) = self.inner.get(i) else {
                return Err(crate::error::Error::Config(format!(
                    "batch acker: index {i} out of range (len {})",
                    self.inner.len()
                )));
            };
            a.nack().await?;
        }
        Ok(())
    }
}

impl<A: Acker> Acker for BatchAcker<A> {
    async fn ack(&self) -> Result<()> {
        for a in self.inner.iter() {
            a.ack().await?;
        }
        Ok(())
    }

    async fn nack(&self) -> Result<()> {
        for a in self.inner.iter() {
            a.nack().await?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct BatchCursor<C> {
    inner: Arc<Vec<C>>,
}

impl<C> BatchCursor<C> {
    pub fn cursors(&self) -> &[C] {
        &self.inner
    }
}

impl<C> Clone for BatchCursor<C> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<C: Cursor> Cursor for BatchCursor<C> {
    fn id(&self) -> CursorId {
        self.inner
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
    async fn batch_reader_groups_messages_by_batch_size() {
        let events: Vec<Event> = (0..5).map(|i| ev(&format!("k{i}"))).collect();
        let reader = VecReader {
            events: Mutex::new(Some(events)),
        };
        let batched = BatchReader::new(reader, NonZeroUsize::new(2).unwrap());
        let mut stream = batched.read(()).await.unwrap();
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

    #[tokio::test]
    async fn batch_cursor_first_cursor_id() {
        let cursor = BatchCursor {
            inner: Arc::new(vec![TestCursor(10), TestCursor(20)]),
        };
        assert_eq!(cursor.id(), TestCursor(10).id());
    }

    #[tokio::test]
    async fn batch_cursor_empty_global() {
        let cursor: BatchCursor<TestCursor> = BatchCursor {
            inner: Arc::new(vec![]),
        };
        assert_eq!(cursor.id(), CursorId::Global);
    }

    #[derive(Clone, Default)]
    struct CountingAcker {
        count: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Acker for CountingAcker {
        async fn ack(&self) -> Result<()> {
            self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        async fn nack(&self) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn batch_acker_ack_subset_only_acks_selected() {
        let ackers: Vec<CountingAcker> = (0..3).map(|_| CountingAcker::default()).collect();
        let snapshot: Vec<CountingAcker> = ackers.to_vec();
        let acker = BatchAcker {
            inner: Arc::new(ackers),
        };
        acker.ack_subset(&[0, 2]).await.unwrap();
        assert_eq!(
            snapshot[0].count.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(
            snapshot[1].count.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert_eq!(
            snapshot[2].count.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }

    #[tokio::test]
    async fn batch_acker_ack_subset_rejects_out_of_range() {
        let acker: BatchAcker<NoopAcker> = BatchAcker {
            inner: Arc::new(vec![NoopAcker]),
        };
        assert!(acker.ack_subset(&[5]).await.is_err());
    }
}
