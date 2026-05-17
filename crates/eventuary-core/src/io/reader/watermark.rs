use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::StreamExt;
use tokio::sync::Mutex;
use tokio::sync::mpsc;

use crate::error::Result;
use crate::event::Event;
use crate::io::stream::SpawnedStream;
use crate::io::{Acker, Message, Reader};

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

pub type KeyFn = dyn Fn(&Event) -> String + Send + Sync;

pub struct WatermarkReader<R, S> {
    inner: R,
    store: S,
    key_fn: Arc<KeyFn>,
}

impl<R, S> WatermarkReader<R, S> {
    pub fn with_static_key(inner: R, store: S, key: impl Into<String>) -> Self {
        let key: Arc<str> = Arc::from(key.into());
        let owned = Arc::clone(&key);
        Self {
            inner,
            store,
            key_fn: Arc::new(move |_| owned.to_string()),
        }
    }

    pub fn with_key_fn<F>(inner: R, store: S, key_fn: F) -> Self
    where
        F: Fn(&Event) -> String + Send + Sync + 'static,
    {
        Self {
            inner,
            store,
            key_fn: Arc::new(key_fn),
        }
    }

    pub fn new(inner: R, store: S, key: impl Into<String>) -> Self {
        Self::with_static_key(inner, store, key)
    }
}

type WatermarkCache = Arc<Mutex<HashMap<String, Option<DateTime<Utc>>>>>;

impl<R, S> Reader for WatermarkReader<R, S>
where
    R: Reader + Send + Sync + 'static,
    R::Subscription: Send + 'static,
    R::Acker: Send + Sync + 'static,
    R::Cursor: Send + Sync + 'static,
    R::Stream: Send + 'static,
    S: WatermarkStore + 'static,
{
    type Subscription = R::Subscription;
    type Acker = WatermarkAcker<R::Acker, S>;
    type Cursor = R::Cursor;
    type Stream = SpawnedStream<WatermarkAcker<R::Acker, S>, R::Cursor>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let inner = self.inner.read(subscription).await?;
        let store = self.store.clone();
        let key_fn = Arc::clone(&self.key_fn);
        let cache: WatermarkCache = Arc::new(Mutex::new(HashMap::new()));
        let (tx, rx) = mpsc::channel(64);

        let intake_cache = Arc::clone(&cache);
        let intake_store = store.clone();
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
                let key = (key_fn)(msg.event());
                let event_ts = msg.event().timestamp();

                let current = {
                    let mut guard = intake_cache.lock().await;
                    if let Some(wm) = guard.get(&key) {
                        *wm
                    } else {
                        let loaded = match intake_store.load_watermark(&key).await {
                            Ok(v) => v,
                            Err(e) => {
                                let _ = tx.send(Err(e)).await;
                                return;
                            }
                        };
                        guard.insert(key.clone(), loaded);
                        loaded
                    }
                };

                if let Some(wm) = current
                    && event_ts <= wm
                {
                    if let Err(e) = msg.ack().await {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                    continue;
                }

                let (event, inner_acker, cursor) = msg.into_parts();
                let wrapped = Message::new(
                    event,
                    WatermarkAcker {
                        inner: inner_acker,
                        store: store.clone(),
                        cache: Arc::clone(&intake_cache),
                        key: Arc::from(key),
                        event_ts,
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

pub struct WatermarkAcker<A: Acker, S: WatermarkStore> {
    inner: A,
    store: S,
    cache: WatermarkCache,
    key: Arc<str>,
    event_ts: DateTime<Utc>,
}

impl<A: Acker, S: WatermarkStore> Acker for WatermarkAcker<A, S> {
    async fn ack(&self) -> Result<()> {
        self.store.save_watermark(&self.key, self.event_ts).await?;
        {
            let mut guard = self.cache.lock().await;
            let entry = guard.entry(self.key.to_string()).or_insert(None);
            if entry.is_none_or(|wm| wm < self.event_ts) {
                *entry = Some(self.event_ts);
            }
        }
        self.inner.ack().await
    }

    async fn nack(&self) -> Result<()> {
        self.inner.nack().await
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;
    use std::time::Duration;

    use chrono::TimeDelta;
    use futures::{Stream, StreamExt, stream};

    use super::*;
    use crate::event::{Event, EventId};
    use crate::io::Message;
    use crate::io::acker::NoopAcker;
    use crate::metadata::Metadata;
    use crate::namespace::Namespace;
    use crate::organization::OrganizationId;
    use crate::payload::Payload;
    use crate::topic::Topic;

    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    struct TestCursor(u64);

    struct VecReader {
        events: StdMutex<Option<Vec<Event>>>,
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
        inner: Arc<StdMutex<HashMap<String, DateTime<Utc>>>>,
    }

    impl WatermarkStore for InMemoryWatermarkStore {
        async fn load_watermark(&self, key: &str) -> Result<Option<DateTime<Utc>>> {
            Ok(self.inner.lock().unwrap().get(key).copied())
        }

        async fn save_watermark(&self, key: &str, ts: DateTime<Utc>) -> Result<()> {
            self.inner.lock().unwrap().insert(key.to_owned(), ts);
            Ok(())
        }
    }

    #[tokio::test]
    async fn older_events_are_skipped() {
        let now = Utc::now();
        let old = now - TimeDelta::seconds(60);
        let recent = now - TimeDelta::seconds(10);

        let store = InMemoryWatermarkStore::default();
        store
            .inner
            .lock()
            .unwrap()
            .insert("test".into(), recent - TimeDelta::seconds(1));

        let reader = VecReader {
            events: StdMutex::new(Some(vec![ev("old", old), ev("recent", recent)])),
        };
        let reader = WatermarkReader::new(reader, store, "test");
        let mut stream = reader.read(()).await.unwrap();

        let msg = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(msg.event().topic().as_str(), "recent");
    }

    #[tokio::test]
    async fn ack_updates_watermark_nack_does_not() {
        let now = Utc::now();
        let first = now - TimeDelta::seconds(60);
        let second = now - TimeDelta::seconds(30);
        let store = InMemoryWatermarkStore::default();

        let reader = WatermarkReader::new(
            VecReader {
                events: StdMutex::new(Some(vec![ev("first", first), ev("second", second)])),
            },
            store.clone(),
            "test",
        );
        let mut stream = reader.read(()).await.unwrap();

        let m1 = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        m1.nack().await.unwrap();
        assert!(store.inner.lock().unwrap().is_empty());

        let m2 = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        m2.ack().await.unwrap();
        assert_eq!(
            store.inner.lock().unwrap().get("test").copied(),
            Some(second)
        );
    }

    #[tokio::test]
    async fn per_entity_key_isolates_watermarks() {
        let now = Utc::now();
        let a_ts = now - TimeDelta::seconds(30);
        let b_ts = now - TimeDelta::seconds(60);
        let store = InMemoryWatermarkStore::default();
        store
            .inner
            .lock()
            .unwrap()
            .insert("a".into(), a_ts + TimeDelta::seconds(1));

        let reader = WatermarkReader::with_key_fn(
            VecReader {
                events: StdMutex::new(Some(vec![ev("a", a_ts), ev("b", b_ts)])),
            },
            store,
            |event| event.topic().as_str().to_owned(),
        );
        let mut stream = reader.read(()).await.unwrap();
        let msg = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(msg.event().topic().as_str(), "b");
    }
}
