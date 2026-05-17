use std::collections::{HashSet, VecDeque};
use std::num::NonZeroUsize;

use futures::StreamExt;
use tokio::sync::mpsc;

use crate::error::Result;
use crate::event::EventId;
use crate::io::stream::SpawnedStream;
use crate::io::{Acker, Cursor, CursorId, Message, Reader};

pub type ReplayThenLiveStream<RA, LA, RC, LC> =
    SpawnedStream<ReplayLiveAcker<RA, LA>, ReplayLiveCursor<RC, LC>>;

#[derive(Debug, Clone, Default)]
pub struct ReplayThenLiveConfig {
    pub overlap_dedupe_capacity: Option<NonZeroUsize>,
}

pub struct ReplayThenLiveReader<R, L> {
    historical: R,
    live: L,
    config: ReplayThenLiveConfig,
}

impl<R, L> ReplayThenLiveReader<R, L> {
    pub fn new(historical: R, live: L) -> Self {
        Self {
            historical,
            live,
            config: ReplayThenLiveConfig::default(),
        }
    }

    pub fn with_config(historical: R, live: L, config: ReplayThenLiveConfig) -> Self {
        Self {
            historical,
            live,
            config,
        }
    }
}

pub struct ReplayThenLiveSubscription<RS, LS> {
    pub replay: RS,
    pub live: LS,
}

impl<RS, LS> ReplayThenLiveSubscription<RS, LS> {
    pub fn new(replay: RS, live: LS) -> Self {
        Self { replay, live }
    }
}

pub enum ReplayLiveCursor<RC, LC> {
    Replay(RC),
    Live(LC),
}

impl<RC: Cursor, LC: Cursor> Cursor for ReplayLiveCursor<RC, LC> {
    fn id(&self) -> CursorId {
        match self {
            Self::Replay(c) => c.id(),
            Self::Live(c) => c.id(),
        }
    }
}

pub enum ReplayLiveAcker<RA: Acker, LA: Acker> {
    Replay(RA),
    Live(LA),
}

impl<RA: Acker, LA: Acker> Acker for ReplayLiveAcker<RA, LA> {
    async fn ack(&self) -> Result<()> {
        match self {
            Self::Replay(a) => a.ack().await,
            Self::Live(a) => a.ack().await,
        }
    }

    async fn nack(&self) -> Result<()> {
        match self {
            Self::Replay(a) => a.nack().await,
            Self::Live(a) => a.nack().await,
        }
    }
}

impl<R, L> Reader for ReplayThenLiveReader<R, L>
where
    R: Reader + Send + Sync + 'static,
    L: Reader + Send + Sync + 'static,
    R::Subscription: Send + 'static,
    L::Subscription: Send + 'static,
    R::Acker: Send + Sync + 'static,
    L::Acker: Send + Sync + 'static,
    R::Cursor: Send + Sync + 'static,
    L::Cursor: Send + Sync + 'static,
    R::Stream: Send + 'static,
    L::Stream: Send + 'static,
{
    type Subscription = ReplayThenLiveSubscription<R::Subscription, L::Subscription>;
    type Acker = ReplayLiveAcker<R::Acker, L::Acker>;
    type Cursor = ReplayLiveCursor<R::Cursor, L::Cursor>;
    type Stream = ReplayThenLiveStream<R::Acker, L::Acker, R::Cursor, L::Cursor>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let replay = self.historical.read(subscription.replay).await?;
        let live = self.live.read(subscription.live).await?;
        let capacity = self.config.overlap_dedupe_capacity;
        let (tx, rx) = mpsc::channel(64);

        let handle = tokio::spawn(async move {
            let mut replay = Box::pin(replay);
            let mut seen: SeenIds = SeenIds::new(capacity);

            while let Some(item) = replay.next().await {
                let msg = match item {
                    Ok(m) => m,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };
                if capacity.is_some() {
                    seen.record(msg.event().id());
                }
                let (event, acker, cursor) = msg.into_parts();
                let wrapped = Message::new(
                    event,
                    ReplayLiveAcker::Replay(acker),
                    ReplayLiveCursor::Replay(cursor),
                );
                if tx.send(Ok(wrapped)).await.is_err() {
                    return;
                }
            }

            let mut live = Box::pin(live);
            while let Some(item) = live.next().await {
                let msg = match item {
                    Ok(m) => m,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };
                if capacity.is_some() && seen.contains(msg.event().id()) {
                    if let Err(e) = msg.ack().await {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                    continue;
                }
                let (event, acker, cursor) = msg.into_parts();
                let wrapped = Message::new(
                    event,
                    ReplayLiveAcker::Live(acker),
                    ReplayLiveCursor::Live(cursor),
                );
                if tx.send(Ok(wrapped)).await.is_err() {
                    return;
                }
            }
        });

        Ok(SpawnedStream::new(rx, handle))
    }
}

struct SeenIds {
    set: HashSet<EventId>,
    order: VecDeque<EventId>,
    capacity: Option<NonZeroUsize>,
}

impl SeenIds {
    fn new(capacity: Option<NonZeroUsize>) -> Self {
        Self {
            set: HashSet::new(),
            order: VecDeque::new(),
            capacity,
        }
    }

    fn record(&mut self, id: EventId) {
        if !self.set.insert(id) {
            return;
        }
        self.order.push_back(id);
        if let Some(cap) = self.capacity {
            while self.set.len() > cap.get() {
                if let Some(oldest) = self.order.pop_front() {
                    self.set.remove(&oldest);
                } else {
                    break;
                }
            }
        }
    }

    fn contains(&self, id: EventId) -> bool {
        self.set.contains(&id)
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
            Ok(Box::pin(stream::iter(events.into_iter().enumerate().map(
                |(i, e)| Ok(Message::new(e, NoopAcker, TestCursor(i as u64))),
            ))))
        }
    }

    fn ev() -> Event {
        Event::create("org", "/x", "thing.happened", Payload::from_string("p")).unwrap()
    }

    #[tokio::test]
    async fn replay_then_live() {
        let historical = VecReader {
            events: Mutex::new(Some(vec![ev(), ev()])),
        };
        let live = VecReader {
            events: Mutex::new(Some(vec![ev()])),
        };
        let reader = ReplayThenLiveReader::new(historical, live);
        let sub = ReplayThenLiveSubscription::new((), ());
        let mut stream = reader.read(sub).await.unwrap();

        let m1 = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(m1.cursor(), ReplayLiveCursor::Replay(_)));
        m1.ack().await.unwrap();

        let m2 = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(m2.cursor(), ReplayLiveCursor::Replay(_)));
        m2.ack().await.unwrap();

        let m3 = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(m3.cursor(), ReplayLiveCursor::Live(_)));
        m3.ack().await.unwrap();

        let end = tokio::time::timeout(Duration::from_millis(200), stream.next()).await;
        assert!(end.is_err() || end.unwrap().is_none());
    }

    #[tokio::test]
    async fn overlap_dedupe_skips_live_events_seen_in_replay() {
        let shared = ev();
        let live_only = ev();
        let historical = VecReader {
            events: Mutex::new(Some(vec![shared.clone()])),
        };
        let live = VecReader {
            events: Mutex::new(Some(vec![shared, live_only.clone()])),
        };
        let reader = ReplayThenLiveReader::with_config(
            historical,
            live,
            ReplayThenLiveConfig {
                overlap_dedupe_capacity: Some(NonZeroUsize::new(128).unwrap()),
            },
        );
        let mut stream = reader
            .read(ReplayThenLiveSubscription::new((), ()))
            .await
            .unwrap();

        let m1 = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(m1.cursor(), ReplayLiveCursor::Replay(_)));
        m1.ack().await.unwrap();

        let m2 = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(m2.cursor(), ReplayLiveCursor::Live(_)));
        assert_eq!(m2.event().id(), live_only.id());
    }
}
