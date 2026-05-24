use std::future::Future;
use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::mpsc;

use crate::error::Result;
use crate::event::Event;
use crate::io::acker::NackContext;
use crate::io::stream::SpawnedStream;
use crate::io::{Acker, Message, Reader};
use crate::payload::Payload;

pub trait DedupeStore<P = Payload>: Clone + Send + Sync + 'static
where
    P: Send + Sync,
{
    fn exists(&self, event: &Event<P>) -> impl Future<Output = Result<bool>> + Send;
    fn mark_processed(&self, event: &Event<P>) -> impl Future<Output = Result<()>> + Send;

    /// Atomically marks the event as seen, returning whether it was newly
    /// recorded (`true`) or was already present (`false`). The default
    /// implementation calls `exists` then `mark_processed`, which is racy
    /// across concurrent callers. Backends with native atomicity (e.g.,
    /// Postgres `INSERT ON CONFLICT DO NOTHING`) should override.
    fn mark_if_new(&self, event: &Event<P>) -> impl Future<Output = Result<bool>> + Send {
        async move {
            if self.exists(event).await? {
                return Ok(false);
            }
            self.mark_processed(event).await?;
            Ok(true)
        }
    }
}

pub struct DedupeReader<R, S> {
    inner: R,
    store: S,
}

impl<R, S> DedupeReader<R, S> {
    pub fn new(inner: R, store: S) -> Self {
        Self { inner, store }
    }
}

impl<R, S, P> Reader<P> for DedupeReader<R, S>
where
    R: Reader<P> + Send + Sync + 'static,
    R::Subscription: Send + 'static,
    R::Acker: Send + Sync + 'static,
    R::Cursor: Send + Sync + 'static,
    R::Stream: Send + 'static,
    S: DedupeStore<P> + 'static,
    P: Clone + Send + Sync + 'static,
{
    type Subscription = R::Subscription;
    type Acker = DedupeAcker<R::Acker, S, P>;
    type Cursor = R::Cursor;
    type Stream = SpawnedStream<DedupeAcker<R::Acker, S, P>, R::Cursor, P>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let inner = self.inner.read(subscription).await?;
        let store = self.store.clone();
        let (tx, rx) = mpsc::channel(64);

        let handle = tokio::spawn(async move {
            let mut stream = Box::pin(inner);
            while let Some(item) = stream.next().await {
                let msg = match item {
                    Ok(m) => m,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };
                match store.exists(msg.event()).await {
                    Ok(true) => {
                        if let Err(e) = msg.ack().await {
                            let _ = tx.send(Err(e)).await;
                            return;
                        }
                        continue;
                    }
                    Ok(false) => {}
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                }
                let (event, inner_acker, cursor) = msg.into_parts();
                let event_arc = Arc::new(event);
                let wrapped = Message::new(
                    (*event_arc).clone(),
                    DedupeAcker {
                        inner: inner_acker,
                        store: store.clone(),
                        event: event_arc,
                    },
                    cursor,
                );
                if tx.send(Ok(wrapped)).await.is_err() {
                    return;
                }
            }
        });

        Ok(SpawnedStream::new(rx, handle))
    }
}

pub struct DedupeAcker<A: Acker, S, P = Payload> {
    inner: A,
    store: S,
    event: Arc<Event<P>>,
}

impl<A, S, P> Acker for DedupeAcker<A, S, P>
where
    A: Acker,
    S: DedupeStore<P>,
    P: Send + Sync,
{
    async fn ack(&self) -> Result<()> {
        self.store.mark_processed(&self.event).await?;
        self.inner.ack().await
    }

    async fn nack(&self) -> Result<()> {
        self.inner.nack().await
    }

    async fn nack_with(&self, context: NackContext) -> Result<()> {
        self.inner.nack_with(context).await
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

    fn ev(topic: &str) -> Event {
        Event::create("org", "/x", topic, Payload::from_string("p")).unwrap()
    }

    use std::collections::HashSet;

    #[derive(Debug, Clone, Default)]
    struct TestDedupeStore {
        seen: Arc<Mutex<HashSet<String>>>,
    }

    impl TestDedupeStore {
        fn len(&self) -> usize {
            self.seen.lock().unwrap().len()
        }
    }

    impl DedupeStore for TestDedupeStore {
        async fn exists(&self, event: &Event) -> Result<bool> {
            Ok(self.seen.lock().unwrap().contains(&event.id().to_string()))
        }

        async fn mark_processed(&self, event: &Event) -> Result<()> {
            self.seen.lock().unwrap().insert(event.id().to_string());
            Ok(())
        }
    }

    #[tokio::test]
    async fn nack_before_mark_allows_redelivery() {
        let e = Event::create("org", "/x", "t", Payload::from_string("p")).unwrap();
        let store = TestDedupeStore::default();
        let reader = VecReader {
            events: Mutex::new(Some(vec![e.clone()])),
        };
        let dedup = DedupeReader::new(reader, store.clone());
        let mut stream = dedup.read(()).await.unwrap();

        let m = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        m.nack().await.unwrap();
        assert_eq!(store.len(), 0);

        let reader = VecReader {
            events: Mutex::new(Some(vec![e])),
        };
        let dedup = DedupeReader::new(reader, store.clone());
        let mut stream = dedup.read(()).await.unwrap();
        let m = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        m.ack().await.unwrap();
        assert_eq!(store.len(), 1);
    }

    #[tokio::test]
    async fn dedup_skips_already_marked_events() {
        let e1 = ev("t1");
        let e2 = ev("t2");
        let store = TestDedupeStore::default();
        store.mark_processed(&e1).await.unwrap();

        let reader = VecReader {
            events: Mutex::new(Some(vec![e1, e2.clone()])),
        };
        let dedup = DedupeReader::new(reader, store);
        let mut stream = dedup.read(()).await.unwrap();

        let m = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(m.event().id(), e2.id());
    }

    #[tokio::test]
    async fn mark_if_new_default_returns_correct_status() {
        let store = TestDedupeStore::default();
        let e = ev("t");
        assert!(store.mark_if_new(&e).await.unwrap());
        assert!(!store.mark_if_new(&e).await.unwrap());
    }
}
