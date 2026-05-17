use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;

use crate::error::Result;
use crate::io::{Acker, Cursor, CursorId, Message, Reader};

pub struct ReplayThenLiveReader<R, L> {
    historical: R,
    live: L,
}

impl<R, L> ReplayThenLiveReader<R, L> {
    pub fn new(historical: R, live: L) -> Self {
        Self { historical, live }
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
    type Stream = ReplayThenLiveStream<R, L>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let replay = self.historical.read(subscription.replay).await?;
        let live = self.live.read(subscription.live).await?;
        Ok(ReplayThenLiveStream {
            replay: Box::pin(replay),
            live: Some(Box::pin(live)),
            phase: Phase::Replay,
        })
    }
}

enum Phase {
    Replay,
    Live,
}

pub struct ReplayThenLiveStream<R: Reader, L: Reader> {
    replay: Pin<Box<R::Stream>>,
    live: Option<Pin<Box<L::Stream>>>,
    phase: Phase,
}

impl<R, L> Stream for ReplayThenLiveStream<R, L>
where
    R: Reader + Send,
    L: Reader + Send,
    R::Acker: Send + Sync,
    L::Acker: Send + Sync,
    R::Cursor: Send + Sync,
    L::Cursor: Send + Sync,
{
    type Item = Result<
        Message<ReplayLiveAcker<R::Acker, L::Acker>, ReplayLiveCursor<R::Cursor, L::Cursor>>,
    >;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.phase {
                Phase::Replay => match self.replay.as_mut().poll_next(cx) {
                    Poll::Ready(Some(Ok(msg))) => {
                        let (event, acker, cursor) = msg.into_parts();
                        return Poll::Ready(Some(Ok(Message::new(
                            event,
                            ReplayLiveAcker::Replay(acker),
                            ReplayLiveCursor::Replay(cursor),
                        ))));
                    }
                    Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                    Poll::Ready(None) => {
                        self.phase = Phase::Live;
                        continue; // re-poll with Live phase
                    }
                    Poll::Pending => return Poll::Pending,
                },
                Phase::Live => {
                    if let Some(live) = &mut self.live {
                        match live.as_mut().poll_next(cx) {
                            Poll::Ready(Some(Ok(msg))) => {
                                let (event, acker, cursor) = msg.into_parts();
                                return Poll::Ready(Some(Ok(Message::new(
                                    event,
                                    ReplayLiveAcker::Live(acker),
                                    ReplayLiveCursor::Live(cursor),
                                ))));
                            }
                            Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                            Poll::Ready(None) => return Poll::Ready(None),
                            Poll::Pending => return Poll::Pending,
                        }
                    } else {
                        return Poll::Ready(None);
                    }
                }
            }
        }
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
}
