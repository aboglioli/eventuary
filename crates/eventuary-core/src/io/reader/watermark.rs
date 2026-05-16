use std::future::Future;

use chrono::{DateTime, Utc};
use futures::StreamExt;
use tokio::sync::mpsc;

use crate::error::Result;
use crate::io::stream::SpawnedStream;
use crate::io::{Reader};

pub trait WatermarkStore: Clone + Send + Sync + 'static {
    fn load_watermark(
        &self,
        key: &str,
    ) -> impl Future<Output = Result<Option<DateTime<Utc>>>> + Send;
    fn save_watermark(
        &self,
        key: &str,
        ts: DateTime<Utc>,
    ) -> impl Future<Output = Result<()>> + Send;
}

pub struct WatermarkReader<R, S> {
    inner: R,
    store: S,
    key: String,
}

impl<R, S> WatermarkReader<R, S> {
    pub fn new(inner: R, store: S, key: impl Into<String>) -> Self {
        Self {
            inner,
            store,
            key: key.into(),
        }
    }
}

impl<R, S> Reader for WatermarkReader<R, S>
where
    R: Reader + Send + Sync + 'static,
    R::Subscription: Send + 'static,
    R::Acker: Send + Sync + 'static,
    R::Cursor: Send + Sync + 'static,
    R::Stream: Send + 'static,
    S: WatermarkStore + 'static,
{
    type Acker = R::Acker;
    type Cursor = R::Cursor;
    type Stream = SpawnedStream<R::Acker, R::Cursor>;
    type Subscription = R::Subscription;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let watermark = self.store.load_watermark(&self.key).await?;
        let inner = self.inner.read(subscription).await?;
        let store = self.store.clone();
        let key = self.key.clone();
        let (tx, rx) = mpsc::channel(64);

        let handle = tokio::spawn(async move {
            let mut stream = Box::pin(inner);
            let mut current_watermark = watermark;

            while let Some(item) = stream.next().await {
                let msg = match item {
                    Ok(m) => m,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };
                let event_ts = msg.event().timestamp();
                if let Some(wm) = current_watermark
                    && event_ts <= wm
                {
                    // Event is older than watermark — ack and skip
                    if let Err(e) = msg.ack().await {
                        let _ = tx.send(Err(e)).await;
                    }
                    continue;
                }
                // Track the latest timestamp for watermark update
                current_watermark = Some(event_ts);
                if let Err(e) = store.save_watermark(&key, event_ts).await {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
                if tx.send(Ok(msg)).await.is_err() {
                    return;
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
    use std::sync::Arc;
    use std::time::Duration;

    use chrono::TimeDelta;
    use futures::{Stream, StreamExt, stream};

    use super::*;
    use crate::event::{Event, EventId};
    use crate::io::acker::NoopAcker;
    use crate::io::{Cursor, Message};
    use crate::metadata::Metadata;
    use crate::namespace::Namespace;
    use crate::organization::OrganizationId;
    use crate::payload::Payload;
    use crate::topic::Topic;

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

    fn ev(topic: &str, ts: DateTime<Utc>) -> Event {
        Event::new(
            EventId::new(),
            OrganizationId::new("org").unwrap(),
            Namespace::new("/x").unwrap(),
            Topic::new(topic).unwrap(),
            Payload::from_string("p"),
            Metadata::new(),
            ts,
            1,
            None,
            None,
            None,
            None,
        )
        .unwrap()
    }

    #[derive(Clone, Default)]
    struct InMemoryWatermarkStore {
        inner: Arc<Mutex<Option<DateTime<Utc>>>>,
    }

    impl WatermarkStore for InMemoryWatermarkStore {
        async fn load_watermark(&self, _: &str) -> Result<Option<DateTime<Utc>>> {
            Ok(*self.inner.lock().unwrap())
        }

        async fn save_watermark(&self, _: &str, ts: DateTime<Utc>) -> Result<()> {
            *self.inner.lock().unwrap() = Some(ts);
            Ok(())
        }
    }

    #[tokio::test]
    async fn older_events_are_skipped() {
        let now = Utc::now();
        let old = now - TimeDelta::seconds(60);
        let recent = now - TimeDelta::seconds(10);

        let store = InMemoryWatermarkStore::default();
        // Watermark set slightly before recent so old is filtered but recent is not
        *store.inner.lock().unwrap() = Some(recent - TimeDelta::seconds(1));

        let reader = VecReader {
            events: Mutex::new(Some(vec![ev("old", old), ev("recent", recent)])),
        };
        let reader = WatermarkReader::new(reader, store, "test");
        let mut stream = reader.read(()).await.unwrap();

        // old should be skipped (older than watermark), recent should arrive
        let msg = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(msg.event().topic().as_str(), "recent");
    }

    #[tokio::test]
    async fn ack_updates_watermark() {
        let now = Utc::now();
        let old = now - TimeDelta::seconds(60);
        let recent = now - TimeDelta::seconds(10);
        let store = InMemoryWatermarkStore::default();

        // Read old event (no watermark set → delivers)
        let reader = WatermarkReader::new(
            VecReader {
                events: Mutex::new(Some(vec![ev("old", old)])),
            },
            store.clone(),
            "test",
        );
        let mut stream = reader.read(()).await.unwrap();
        let msg = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(msg.event().topic().as_str(), "old");
        msg.ack().await.unwrap();

        // Read again: old should be skipped, recent delivered
        let reader = WatermarkReader::new(
            VecReader {
                events: Mutex::new(Some(vec![ev("old", old), ev("recent", recent)])),
            },
            store.clone(),
            "test",
        );
        let mut stream = reader.read(()).await.unwrap();
        let msg = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(msg.event().topic().as_str(), "recent");
    }
}
