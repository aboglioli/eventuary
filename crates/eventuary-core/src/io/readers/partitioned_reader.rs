//! Partitioned reader: in-process lane scheduler over any inner reader.
//!
//! Routes inner messages into N lanes by `partition_for(event, count)`.
//! Each lane buffers up to `lane_capacity` events. The intake task acks
//! the inner message as soon as the event is accepted into its lane, so
//! source-level progress advances immediately. Downstream redelivery is
//! in-memory only: a downstream `nack` puts the event back at the head of
//! its lane, a downstream `ack` clears the lane's in-flight slot so the
//! merged emit task can serve the next event from that lane (or any
//! other ready lane).
//!
//! Lane scheduling:
//!   - `RoundRobin`: cycle through lanes one event each.
//!   - `QueueDepthWeighted { max_burst_per_lane }`: pick the deepest ready
//!     lane and serve up to `max_burst_per_lane` events from it before
//!     rotating, so hot lanes drain quickly without starving cold lanes.
//!
//! Back-pressure / jam: if all lanes are at capacity AND no lane has an
//! in-flight message that could drain after a downstream ack, the stream
//! errors. While at least one lane is making progress, the intake task
//! waits and emits a `tracing::warn!`.

use std::collections::VecDeque;
use std::num::{NonZeroU16, NonZeroUsize};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use futures::{Stream, StreamExt};
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::sync::mpsc;

use crate::error::{Error, Result};
use crate::event::Event;
use crate::io::checkpoint::{
    CheckpointResumableSubscription, CheckpointResume, CheckpointResumePoint,
};
use crate::io::{Acker, Message, Reader};
use crate::partition::{CommitCursor, CursorPartition, LogicalPartition, partition_for};

#[derive(Debug, Clone)]
pub struct PartitionedReaderConfig {
    pub partition_count: NonZeroU16,
    pub lane_capacity: NonZeroUsize,
    pub scheduling: LaneScheduling,
}

impl Default for PartitionedReaderConfig {
    fn default() -> Self {
        Self {
            partition_count: NonZeroU16::new(64).unwrap(),
            lane_capacity: NonZeroUsize::new(128).unwrap(),
            scheduling: LaneScheduling::QueueDepthWeighted {
                max_burst_per_lane: NonZeroUsize::new(8).unwrap(),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum LaneScheduling {
    RoundRobin,
    QueueDepthWeighted { max_burst_per_lane: NonZeroUsize },
}

#[derive(Debug, Clone)]
pub struct PartitionedSubscription<S, C = crate::io::NoCursor> {
    pub inner: S,
    resume_points: Vec<CheckpointResumePoint<C>>,
}

impl<S, C> PartitionedSubscription<S, C> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            resume_points: Vec::new(),
        }
    }

    pub fn resume_points(&self) -> &[CheckpointResumePoint<C>] {
        &self.resume_points
    }
}

impl<S, C> CheckpointResumableSubscription<C> for PartitionedSubscription<S, C>
where
    S: Clone + Send + 'static,
    C: Clone + Send + 'static,
{
    fn with_checkpoint_resume(mut self, resume: CheckpointResume<C>) -> Self {
        let (_start, points) = resume.into_parts();
        self.resume_points = points;
        self
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PartitionedCursor<C> {
    inner: C,
    partition: LogicalPartition,
}

impl<C> PartitionedCursor<C> {
    pub fn new(inner: C, partition: LogicalPartition) -> Self {
        Self { inner, partition }
    }

    pub fn inner(&self) -> &C {
        &self.inner
    }

    pub fn into_inner(self) -> C {
        self.inner
    }

    pub fn partition(&self) -> LogicalPartition {
        self.partition
    }
}

impl<C> CursorPartition for PartitionedCursor<C> {
    fn partition(&self) -> Option<LogicalPartition> {
        Some(self.partition)
    }
}

impl<C> CommitCursor for PartitionedCursor<C>
where
    C: CommitCursor,
{
    type Commit = C::Commit;
    fn commit_cursor(&self) -> Self::Commit {
        self.inner.commit_cursor()
    }
}

struct InFlightItem<C> {
    id: u64,
    event: Event,
    cursor: C,
}

struct Lane<C> {
    queue: VecDeque<BufferedItem<C>>,
    in_flight: Option<InFlightItem<C>>,
    capacity: usize,
    burst_consumed: usize,
}

struct BufferedItem<C> {
    event: Event,
    cursor: C,
}

struct Lanes<C> {
    lanes: Vec<Lane<C>>,
    next_id: u64,
    last_served: usize,
}

pub struct PartitionAcker<C: Clone + Send + Sync + 'static> {
    state: Arc<Mutex<Lanes<C>>>,
    notify: Arc<Notify>,
    lane_id: usize,
    id: u64,
}

impl<C> Acker for PartitionAcker<C>
where
    C: Clone + Send + Sync + 'static,
{
    async fn ack(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        let lane = &mut state.lanes[self.lane_id];
        if matches!(&lane.in_flight, Some(f) if f.id == self.id) {
            lane.in_flight = None;
        }
        drop(state);
        self.notify.notify_waiters();
        Ok(())
    }

    async fn nack(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        let lane = &mut state.lanes[self.lane_id];
        if let Some(in_flight) = lane.in_flight.take()
            && in_flight.id == self.id
        {
            lane.queue.push_front(BufferedItem {
                event: in_flight.event,
                cursor: in_flight.cursor,
            });
        }
        drop(state);
        self.notify.notify_waiters();
        Ok(())
    }
}

pub struct PartitionedStream<C: Clone + Send + Sync + 'static> {
    rx: mpsc::Receiver<Result<Message<PartitionAcker<C>, PartitionedCursor<C>>>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl<C: Clone + Send + Sync + 'static> Drop for PartitionedStream<C> {
    fn drop(&mut self) {
        if let Some(h) = self.handle.take() {
            h.abort();
        }
    }
}

impl<C: Clone + Send + Sync + 'static> Stream for PartitionedStream<C> {
    type Item = Result<Message<PartitionAcker<C>, PartitionedCursor<C>>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

pub struct PartitionedReader<R> {
    inner: R,
    config: PartitionedReaderConfig,
}

impl<R> PartitionedReader<R> {
    pub fn new(inner: R, config: PartitionedReaderConfig) -> Self {
        Self { inner, config }
    }
}

impl<R> Reader for PartitionedReader<R>
where
    R: Reader + Send + Sync + 'static,
    R::Subscription: Clone + Send + 'static,
    R::Stream: Send + 'static,
    R::Acker: Acker + Send + Sync + 'static,
    R::Cursor: Clone + Send + Sync + 'static,
    R::Cursor: CommitCursor,
    <R::Cursor as CommitCursor>::Commit: Clone + Ord + Send + Sync + 'static,
    R::Subscription: CheckpointResumableSubscription<<R::Cursor as CommitCursor>::Commit>,
{
    type Subscription =
        PartitionedSubscription<R::Subscription, <R::Cursor as CommitCursor>::Commit>;
    type Acker = PartitionAcker<R::Cursor>;
    type Cursor = PartitionedCursor<R::Cursor>;
    type Stream = PartitionedStream<R::Cursor>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let inner_stream = self.inner.read(subscription.inner).await?;
        let count_nz = self.config.partition_count;
        let count = count_nz.get() as usize;
        let count_u32 = std::num::NonZeroU32::new(count_nz.get() as u32).unwrap();
        let lane_capacity = self.config.lane_capacity.get();
        let scheduling = self.config.scheduling;

        let lanes_inner: Vec<Lane<R::Cursor>> = (0..count)
            .map(|_| Lane {
                queue: VecDeque::new(),
                in_flight: None,
                capacity: lane_capacity,
                burst_consumed: 0,
            })
            .collect();
        let state: Arc<Mutex<Lanes<R::Cursor>>> = Arc::new(Mutex::new(Lanes {
            lanes: lanes_inner,
            next_id: 0,
            last_served: 0,
        }));
        let notify = Arc::new(Notify::new());

        let (tx, rx) = mpsc::channel::<
            Result<Message<PartitionAcker<R::Cursor>, PartitionedCursor<R::Cursor>>>,
        >(64);

        let intake_done = Arc::new(AtomicBool::new(false));
        let intake_state = Arc::clone(&state);
        let intake_notify = Arc::clone(&notify);
        let intake_tx = tx.clone();
        let intake_done_for_task = Arc::clone(&intake_done);
        let intake_handle = tokio::spawn(async move {
            struct DoneGuard {
                done: Arc<AtomicBool>,
                notify: Arc<Notify>,
            }
            impl Drop for DoneGuard {
                fn drop(&mut self) {
                    self.done.store(true, Ordering::SeqCst);
                    self.notify.notify_waiters();
                }
            }
            let _done_guard = DoneGuard {
                done: Arc::clone(&intake_done_for_task),
                notify: Arc::clone(&intake_notify),
            };
            let mut inner_stream = Box::pin(inner_stream);
            while let Some(item) = inner_stream.next().await {
                let msg = match item {
                    Ok(m) => m,
                    Err(e) => {
                        let _ = intake_tx.send(Err(e)).await;
                        continue;
                    }
                };
                let lane_id = partition_for(msg.event(), count_u32) as usize;
                let mut msg_holder = Some(msg);
                loop {
                    let lanes = intake_state.lock().await;
                    let lane = &lanes.lanes[lane_id];
                    if lane.queue.len() < lane.capacity {
                        drop(lanes);
                        let msg = msg_holder.take().expect("msg present");
                        let (event, inner_acker, cursor) = msg.into_parts();
                        // Ack inner source FIRST so a failure terminates
                        // the stream before the event becomes downstream-
                        // visible via a lane buffer.
                        if let Err(e) = inner_acker.ack().await {
                            let _ = intake_tx
                                .send(Err(Error::Store(format!(
                                    "partitioned reader: inner acker failed: {e}"
                                ))))
                                .await;
                            return;
                        }
                        let mut lanes = intake_state.lock().await;
                        lanes.lanes[lane_id]
                            .queue
                            .push_back(BufferedItem { event, cursor });
                        drop(lanes);
                        intake_notify.notify_waiters();
                        break;
                    }
                    let all_full = lanes.lanes.iter().all(|l| l.queue.len() >= l.capacity);
                    let any_inflight = lanes.lanes.iter().any(|l| l.in_flight.is_some());
                    drop(lanes);
                    if all_full && !any_inflight {
                        let _ = intake_tx
                            .send(Err(Error::Store(
                                "partitioned reader: all lanes stuck (full + no in-flight progress)"
                                    .to_owned(),
                            )))
                            .await;
                        return;
                    }
                    tracing::warn!(
                        lane = lane_id,
                        "partitioned reader lane at capacity, waiting"
                    );
                    intake_notify.notified().await;
                }
            }
        });

        let emit_state = Arc::clone(&state);
        let emit_notify = Arc::clone(&notify);
        let emit_tx = tx;
        let emit_handle = tokio::spawn(async move {
            loop {
                let pick = {
                    let mut lanes = emit_state.lock().await;
                    let lane_count = lanes.lanes.len();
                    let burst = match scheduling {
                        LaneScheduling::RoundRobin => 1usize,
                        LaneScheduling::QueueDepthWeighted { max_burst_per_lane } => {
                            max_burst_per_lane.get()
                        }
                    };
                    let last = lanes.last_served;

                    let mut found: Option<usize> = None;
                    let lane_at_last = &mut lanes.lanes[last];
                    if let LaneScheduling::QueueDepthWeighted { .. } = scheduling
                        && lane_at_last.in_flight.is_none()
                        && !lane_at_last.queue.is_empty()
                        && lane_at_last.burst_consumed < burst
                    {
                        found = Some(last);
                    }

                    if found.is_none() {
                        let mut best: Option<(usize, usize)> = None;
                        let mut rr_pick: Option<usize> = None;
                        for offset in 1..=lane_count {
                            let idx = (last + offset) % lane_count;
                            let lane = &lanes.lanes[idx];
                            if lane.in_flight.is_some() || lane.queue.is_empty() {
                                continue;
                            }
                            match scheduling {
                                LaneScheduling::RoundRobin => {
                                    rr_pick = Some(idx);
                                    break;
                                }
                                LaneScheduling::QueueDepthWeighted { .. } => {
                                    let depth = lane.queue.len();
                                    if best.map(|(_, d)| depth > d).unwrap_or(true) {
                                        best = Some((idx, depth));
                                    }
                                }
                            }
                        }
                        found = rr_pick.or(best.map(|(i, _)| i));
                    }

                    if let Some(idx) = found {
                        let id = lanes.next_id;
                        lanes.next_id += 1;
                        let lane = &mut lanes.lanes[idx];
                        let buffered = lane.queue.pop_front().expect("queue non-empty");
                        lane.in_flight = Some(InFlightItem {
                            id,
                            event: buffered.event.clone(),
                            cursor: buffered.cursor.clone(),
                        });
                        if idx == last {
                            lane.burst_consumed += 1;
                        } else {
                            for (i, l) in lanes.lanes.iter_mut().enumerate() {
                                if i != idx {
                                    l.burst_consumed = 0;
                                }
                            }
                            lanes.lanes[idx].burst_consumed = 1;
                            lanes.last_served = idx;
                        }
                        if let LaneScheduling::QueueDepthWeighted { max_burst_per_lane } =
                            scheduling
                            && lanes.lanes[idx].burst_consumed >= max_burst_per_lane.get()
                        {
                            lanes.lanes[idx].burst_consumed = 0;
                        }
                        Some((idx, id, buffered.event, buffered.cursor))
                    } else {
                        None
                    }
                };

                match pick {
                    Some((lane_id, id, event, cursor)) => {
                        let partition =
                            LogicalPartition::new(lane_id as u16, count_nz).expect("valid lane");
                        let acker = PartitionAcker {
                            state: Arc::clone(&emit_state),
                            notify: Arc::clone(&emit_notify),
                            lane_id,
                            id,
                        };
                        let cursor_out = PartitionedCursor::new(cursor, partition);
                        let msg = Message::new(event, acker, cursor_out);
                        if emit_tx.send(Ok(msg)).await.is_err() {
                            return;
                        }
                    }
                    None => {
                        if emit_tx.is_closed() {
                            return;
                        }
                        if intake_done.load(Ordering::SeqCst) {
                            let lanes = emit_state.lock().await;
                            let drained = lanes
                                .lanes
                                .iter()
                                .all(|l| l.queue.is_empty() && l.in_flight.is_none());
                            drop(lanes);
                            if drained {
                                return;
                            }
                        }
                        emit_notify.notified().await;
                    }
                }
            }
        });

        let handle = tokio::spawn(async move {
            let _ = intake_handle.await;
            let _ = emit_handle.await;
        });

        Ok(PartitionedStream {
            rx,
            handle: Some(handle),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Event;
    use crate::io::Message;
    use crate::io::acker::NoopAcker;
    use crate::payload::Payload;
    use std::time::Duration;

    #[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
    struct TestCursor(i64);

    impl crate::partition::CursorPartition for TestCursor {
        fn partition(&self) -> Option<LogicalPartition> {
            None
        }
    }

    impl crate::partition::CommitCursor for TestCursor {
        type Commit = TestCursor;

        fn commit_cursor(&self) -> Self::Commit {
            *self
        }
    }

    impl CheckpointResumableSubscription<TestCursor> for () {
        fn with_checkpoint_resume(self, _: CheckpointResume<TestCursor>) -> Self {
            self
        }
    }

    #[test]
    fn partitioned_subscription_stores_checkpoint_resume_metadata_without_rewriting_inner() {
        let partition = LogicalPartition::new(1, NonZeroU16::new(4).unwrap()).unwrap();
        let subscription =
            PartitionedSubscription::new(()).with_checkpoint_resume(CheckpointResume::new(
                crate::start_from::StartFrom::After(TestCursor(10)),
                vec![CheckpointResumePoint::new(Some(partition), TestCursor(10))],
            ));

        assert_eq!(subscription.resume_points().len(), 1);
        assert_eq!(subscription.resume_points()[0].partition(), Some(partition));
        assert_eq!(*subscription.resume_points()[0].cursor(), TestCursor(10));
    }

    struct VecReader {
        events: std::sync::Mutex<Option<Vec<Event>>>,
    }

    impl Reader for VecReader {
        type Subscription = ();
        type Acker = NoopAcker;
        type Cursor = TestCursor;
        type Stream = Pin<Box<dyn Stream<Item = Result<Message<NoopAcker, TestCursor>>> + Send>>;

        async fn read(&self, _: ()) -> Result<Self::Stream> {
            let events = self.events.lock().unwrap().take().unwrap();
            let iter = events
                .into_iter()
                .enumerate()
                .map(|(i, e)| Ok(Message::new(e, NoopAcker, TestCursor(i as i64 + 1))));
            Ok(Box::pin(futures::stream::iter(iter)))
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

    fn rr_config(n: u16, cap: usize) -> PartitionedReaderConfig {
        PartitionedReaderConfig {
            partition_count: NonZeroU16::new(n).unwrap(),
            lane_capacity: NonZeroUsize::new(cap).unwrap(),
            scheduling: LaneScheduling::RoundRobin,
        }
    }

    #[tokio::test]
    async fn partitions_events_into_lanes() {
        let events = (0..16).map(|i| ev(&format!("k{i}"))).collect::<Vec<_>>();
        let reader = VecReader {
            events: std::sync::Mutex::new(Some(events)),
        };
        let p = PartitionedReader::new(reader, rr_config(4, 64));
        let mut stream = p.read(PartitionedSubscription::new(())).await.unwrap();
        let mut delivered = 0usize;
        while delivered < 16 {
            let msg = tokio::time::timeout(Duration::from_secs(2), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(msg.cursor().partition().id() < 4);
            msg.ack().await.unwrap();
            delivered += 1;
        }
        assert_eq!(delivered, 16);
    }

    #[tokio::test]
    async fn partitioned_reader_does_not_emit_two_unacked_messages_from_same_lane() {
        let events: Vec<Event> = (0..8).map(|_| ev("same-key")).collect();
        let reader = VecReader {
            events: std::sync::Mutex::new(Some(events)),
        };
        let p = PartitionedReader::new(reader, rr_config(4, 64));
        let mut stream = p.read(PartitionedSubscription::new(())).await.unwrap();
        let first = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let target_lane = first.cursor().partition().id();
        let next = tokio::time::timeout(Duration::from_millis(200), stream.next()).await;
        assert!(
            next.is_err(),
            "lane {target_lane} emitted second message before first was acked"
        );
        first.ack().await.unwrap();
        let after_ack = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(after_ack.cursor().partition().id(), target_lane);
        after_ack.ack().await.unwrap();
    }

    #[tokio::test]
    async fn partitioned_reader_redelivers_lane_event_after_nack() {
        let events: Vec<Event> = (0..3).map(|_| ev("same-key")).collect();
        let reader = VecReader {
            events: std::sync::Mutex::new(Some(events)),
        };
        let p = PartitionedReader::new(reader, rr_config(4, 64));
        let mut stream = p.read(PartitionedSubscription::new(())).await.unwrap();
        let first = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let first_id = first.event().id();
        first.nack().await.unwrap();
        let second = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            second.event().id(),
            first_id,
            "nacked event was not re-emitted at head of its lane"
        );
        second.ack().await.unwrap();
    }

    #[tokio::test]
    async fn partitioned_reader_terminates_after_inner_stream_ends() {
        let events: Vec<Event> = (0..4).map(|i| ev(&format!("k{i}"))).collect();
        let reader = VecReader {
            events: std::sync::Mutex::new(Some(events)),
        };
        let p = PartitionedReader::new(reader, rr_config(4, 64));
        let mut stream = p.read(PartitionedSubscription::new(())).await.unwrap();
        let mut delivered = 0usize;
        while delivered < 4 {
            let msg = tokio::time::timeout(Duration::from_secs(2), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            msg.ack().await.unwrap();
            delivered += 1;
        }
        // After all events delivered and acked, stream must end, not hang.
        let end = tokio::time::timeout(Duration::from_secs(2), stream.next()).await;
        match end {
            Ok(None) => {}
            Ok(Some(_)) => panic!("expected stream end after inner exhausted, got extra message"),
            Err(_) => panic!("stream did not terminate after inner exhausted"),
        }
    }

    struct FailingAcker;
    impl Acker for FailingAcker {
        async fn ack(&self) -> Result<()> {
            Err(Error::Store("intake ack failure".into()))
        }
        async fn nack(&self) -> Result<()> {
            Ok(())
        }
    }

    struct FailingAckReader {
        events: std::sync::Mutex<Option<Vec<Event>>>,
    }
    impl Reader for FailingAckReader {
        type Subscription = ();
        type Acker = FailingAcker;
        type Cursor = TestCursor;
        type Stream = Pin<Box<dyn Stream<Item = Result<Message<FailingAcker, TestCursor>>> + Send>>;

        async fn read(&self, _: ()) -> Result<Self::Stream> {
            let events = self.events.lock().unwrap().take().unwrap();
            let iter = events
                .into_iter()
                .enumerate()
                .map(|(i, e)| Ok(Message::new(e, FailingAcker, TestCursor(i as i64 + 1))));
            Ok(Box::pin(futures::stream::iter(iter)))
        }
    }

    #[tokio::test]
    async fn partitioned_reader_surfaces_inner_ack_error() {
        let reader = FailingAckReader {
            events: std::sync::Mutex::new(Some(vec![ev("k0")])),
        };
        let p = PartitionedReader::new(reader, rr_config(4, 64));
        let mut stream = p.read(PartitionedSubscription::new(())).await.unwrap();
        let first = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap();
        assert!(
            first.is_err(),
            "inner ack failure must surface as stream error"
        );
    }

    #[tokio::test]
    async fn partitioned_reader_does_not_deliver_event_whose_inner_ack_failed() {
        // After inner ack fails, the failed event must never be visible
        // downstream — the stream must yield exactly the error and then
        // terminate.
        let reader = FailingAckReader {
            events: std::sync::Mutex::new(Some(vec![ev("k0"), ev("k1")])),
        };
        let p = PartitionedReader::new(reader, rr_config(4, 64));
        let mut stream = p.read(PartitionedSubscription::new(())).await.unwrap();
        let first = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap();
        assert!(first.is_err(), "expected stream error on inner ack failure");
        // Stream must terminate now (intake aborted); no event should
        // ever be delivered.
        let end = tokio::time::timeout(Duration::from_secs(2), stream.next()).await;
        match end {
            Ok(None) => {}
            Ok(Some(Err(_))) => {}
            Ok(Some(Ok(_))) => {
                panic!("event leaked downstream after inner ack failure");
            }
            Err(_) => panic!("stream did not terminate after inner ack failure"),
        }
    }

    #[tokio::test]
    async fn queue_depth_weighted_scheduler_services_hot_lanes_without_starving_cold_lanes() {
        // 32 events to a hot key + 1 event to a cold key. With burst=8,
        // hot lane should serve 8 in a row, then the cold lane must get
        // its turn before hot resumes.
        let mut events: Vec<Event> = (0..32).map(|_| ev("hot")).collect();
        events.push(ev("cold"));
        // shuffle so cold isn't at the end naturally
        // (intake order doesn't matter for lane assignment — both go to
        // their own deterministic lane).
        let reader = VecReader {
            events: std::sync::Mutex::new(Some(events)),
        };
        let cfg = PartitionedReaderConfig {
            partition_count: NonZeroU16::new(4).unwrap(),
            lane_capacity: NonZeroUsize::new(64).unwrap(),
            scheduling: LaneScheduling::QueueDepthWeighted {
                max_burst_per_lane: NonZeroUsize::new(8).unwrap(),
            },
        };
        let p = PartitionedReader::new(reader, cfg);
        let mut stream = p.read(PartitionedSubscription::new(())).await.unwrap();
        let mut order: Vec<String> = Vec::new();
        for _ in 0..33 {
            let msg = tokio::time::timeout(Duration::from_secs(2), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            order.push(msg.event().key().unwrap().as_str().to_owned());
            msg.ack().await.unwrap();
        }
        let cold_pos = order
            .iter()
            .position(|k| k == "cold")
            .expect("cold delivered");
        // cold lane has 1 event, must be served within max_burst_per_lane
        // (8) deliveries from when both lanes are ready — at the latest
        // within the first 16 deliveries (one hot burst + cold).
        assert!(
            cold_pos < 16,
            "cold lane starved: cold delivered at position {cold_pos}, order={order:?}"
        );
    }

    #[tokio::test]
    async fn partitioned_reader_can_emit_other_lanes_when_one_lane_is_unacked() {
        // Use enough events so lanes spread; collect first 4 deliveries
        // and confirm at least two distinct lanes appear without acking
        // the first.
        let events: Vec<Event> = (0..32).map(|i| ev(&format!("k{i}"))).collect();
        let reader = VecReader {
            events: std::sync::Mutex::new(Some(events)),
        };
        let p = PartitionedReader::new(reader, rr_config(4, 64));
        let mut stream = p.read(PartitionedSubscription::new(())).await.unwrap();
        let mut held = Vec::new();
        let mut lanes = std::collections::HashSet::new();
        for _ in 0..4 {
            let msg = tokio::time::timeout(Duration::from_secs(2), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            lanes.insert(msg.cursor().partition().id());
            held.push(msg);
        }
        assert!(
            lanes.len() >= 2,
            "expected at least two distinct lanes to emit without acking, got {lanes:?}"
        );
        for m in held {
            m.ack().await.unwrap();
        }
    }
}
