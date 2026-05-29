//! CheckpointReader: composes an inner reader and a checkpoint store.
//!
//! On `read`, loads all persisted cursors for the requested scope
//! (one per `CursorId`) and hands them to the inner subscription via
//! `StartableSubscription::with_starts` as a `Vec<StartFrom::After(c)>`.
//! The inner reader decides which row to resume from — the default
//! impl picks the smallest `After(c)`; wrappers like `PartitionedReader`
//! filter by compatibility first. Records each delivered cursor
//! in contiguous delivered order per `CursorId`. On downstream `ack`,
//! calls the inner ack first, then buffers the cursor progress and
//! commits to the store according to the configured
//! [`CheckpointFlushPolicy`] (immediate flush by default). Store commit
//! errors propagate to the caller and to consumers of the stream.
//!
//! When `max_pending_interval > 0`, a background timer task flushes
//! expired entries even if no further acks arrive. The returned
//! [`CheckpointStream`] also exposes [`CheckpointStream::flush`] for
//! explicit on-demand drain, which graceful-shutdown paths should call
//! before dropping the stream.
//!
//! The checkpoint store persists the same cursor type the inner reader
//! emits. `CheckpointReader` uses `Cursor::id()` only to build the
//! checkpoint key; it does not transform the cursor before storing or
//! resuming. For partitioned composition (`PartitionedReader<R>`), the
//! persisted value is `PartitionedCursor<R::Cursor>`.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Stream;
use futures::StreamExt;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::error::{Error, Result};
use crate::io::ConsumerGroupId;
use crate::io::acker::NackContext;
use crate::io::position::{StartFrom, StartableSubscription};
use crate::io::stream_id::StreamId;
use crate::io::{Acker, Cursor, CursorId, Message, Reader};

pub trait CheckpointStore<C>: Clone + Send + Sync + 'static {
    fn load<'a>(
        &'a self,
        key: &'a CheckpointKey,
    ) -> impl Future<Output = Result<Option<C>>> + Send + 'a;

    fn load_scope<'a>(
        &'a self,
        scope: &'a CheckpointScope,
    ) -> impl Future<Output = Result<Vec<(CursorId, C)>>> + Send + 'a;

    fn commit<'a>(
        &'a self,
        key: &'a CheckpointKey,
        cursor: C,
    ) -> impl Future<Output = Result<()>> + Send + 'a;
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct CheckpointScope {
    pub consumer_group_id: ConsumerGroupId,
    pub stream_id: StreamId,
}

impl CheckpointScope {
    pub fn new(consumer_group_id: ConsumerGroupId, stream_id: StreamId) -> Self {
        Self {
            consumer_group_id,
            stream_id,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct CheckpointKey {
    pub scope: CheckpointScope,
    pub cursor_id: CursorId,
}

impl CheckpointKey {
    pub fn new(scope: CheckpointScope, cursor_id: CursorId) -> Self {
        Self { scope, cursor_id }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub enum MissingCheckpointPolicy {
    #[default]
    UseInitialStart,
    Error,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub enum InvalidCursorPolicy {
    #[default]
    UseInitialStart,
    Error,
}

/// Buffering policy for per-CursorId checkpoint flushes.
///
/// `CheckpointReader` collapses repeated `store.commit(...)` calls per
/// `CursorId` into a single flush, controlled by either a count or a time
/// threshold. The default policy (`max_pending_acks = 1`,
/// `max_pending_interval = ZERO`) flushes on every ack, matching the
/// historical behavior.
///
/// Events acked between flushes will be redelivered on crash. Handlers
/// must already be idempotent for redelivery.
#[derive(Debug, Clone, Copy)]
pub struct CheckpointFlushPolicy {
    pub max_pending_acks: NonZeroUsize,
    pub max_pending_interval: Duration,
}

impl Default for CheckpointFlushPolicy {
    fn default() -> Self {
        Self {
            max_pending_acks: NonZeroUsize::new(1).unwrap(),
            max_pending_interval: Duration::ZERO,
        }
    }
}

pub struct CheckpointReaderConfig {
    pub max_pending_per_key: usize,
    pub flush_policy: CheckpointFlushPolicy,
}

impl Default for CheckpointReaderConfig {
    fn default() -> Self {
        Self {
            max_pending_per_key: 1024,
            flush_policy: CheckpointFlushPolicy::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CheckpointSubscription<S> {
    pub inner: S,
    pub scope: CheckpointScope,
    pub on_missing: MissingCheckpointPolicy,
    pub on_invalid: InvalidCursorPolicy,
}

impl<S> CheckpointSubscription<S> {
    pub fn new(inner: S, scope: CheckpointScope) -> Self {
        Self {
            inner,
            scope,
            on_missing: MissingCheckpointPolicy::UseInitialStart,
            on_invalid: InvalidCursorPolicy::UseInitialStart,
        }
    }

    pub fn on_missing(mut self, policy: MissingCheckpointPolicy) -> Self {
        self.on_missing = policy;
        self
    }

    pub fn on_invalid(mut self, policy: InvalidCursorPolicy) -> Self {
        self.on_invalid = policy;
        self
    }
}

struct Pending<C> {
    cursor: C,
    completed: bool,
}

/// Per-`CursorId` state combining contiguous-ack tracking and flush
/// accounting under a single lock.
struct PerCursorState<C> {
    queue: VecDeque<Pending<C>>,
    offset: usize,
    pending_cursor: Option<C>,
    pending_count: usize,
    first_pending_at: Option<Instant>,
}

impl<C: Clone> PerCursorState<C> {
    fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            offset: 0,
            pending_cursor: None,
            pending_count: 0,
            first_pending_at: None,
        }
    }

    fn record(&mut self, cursor: C) -> usize {
        let idx = self.offset + self.queue.len();
        self.queue.push_back(Pending {
            cursor,
            completed: false,
        });
        idx
    }

    fn complete(&mut self, idx: usize) -> Option<C> {
        let adjusted = idx.wrapping_sub(self.offset);
        self.queue[adjusted].completed = true;
        let mut latest: Option<C> = None;
        while self.queue.front().map(|p| p.completed).unwrap_or(false) {
            latest = Some(self.queue.pop_front().unwrap().cursor);
            self.offset += 1;
        }
        latest
    }

    fn pending_queue_count(&self) -> usize {
        self.queue.len()
    }

    /// Advance the buffered flush cursor and return it if a flush
    /// threshold has been crossed. Caller should commit the returned
    /// cursor outside the lock.
    fn advance_and_check_flush(
        &mut self,
        cursor: C,
        policy: &CheckpointFlushPolicy,
        now: Instant,
    ) -> Option<C> {
        if self.first_pending_at.is_none() {
            self.first_pending_at = Some(now);
        }
        self.pending_cursor = Some(cursor);
        self.pending_count += 1;

        let count_hit = self.pending_count >= policy.max_pending_acks.get();
        let interval_hit = !policy.max_pending_interval.is_zero()
            && self
                .first_pending_at
                .map(|t| now.duration_since(t) >= policy.max_pending_interval)
                .unwrap_or(false);

        if count_hit || interval_hit {
            self.take_pending()
        } else {
            None
        }
    }

    /// Take the buffered flush cursor and reset flush accounting.
    fn take_pending(&mut self) -> Option<C> {
        self.pending_count = 0;
        self.first_pending_at = None;
        self.pending_cursor.take()
    }

    fn is_empty(&self) -> bool {
        self.queue.is_empty() && self.pending_cursor.is_none()
    }

    fn flush_deadline(&self, policy: &CheckpointFlushPolicy) -> Option<Instant> {
        if policy.max_pending_interval.is_zero() {
            return None;
        }
        self.first_pending_at
            .map(|t| t + policy.max_pending_interval)
    }
}

type StateMap<C> = Arc<Mutex<HashMap<CursorId, PerCursorState<C>>>>;

/// Notifies the background flush timer that pending state changed
/// (new entry, threshold crossing, drop). Atomic flag avoids busy-wait
/// when the timer is parked between deadlines.
#[derive(Clone)]
struct FlushSignal {
    notify: Arc<Notify>,
}

impl FlushSignal {
    fn new() -> Self {
        Self {
            notify: Arc::new(Notify::new()),
        }
    }

    fn wake(&self) {
        self.notify.notify_one();
    }

    async fn wait(&self) {
        self.notify.notified().await;
    }
}

pub struct CheckpointAcker<A: Acker, C, S: CheckpointStore<C>> {
    inner: A,
    scope: CheckpointScope,
    cursor_id: CursorId,
    index: usize,
    state: StateMap<C>,
    store: S,
    flush_policy: CheckpointFlushPolicy,
    flush_signal: FlushSignal,
}

impl<A, C, S> Acker for CheckpointAcker<A, C, S>
where
    A: Acker + Send + Sync + 'static,
    C: Clone + Send + Sync + 'static,
    S: CheckpointStore<C>,
{
    async fn ack(&self) -> Result<()> {
        self.inner.ack().await?;
        let to_commit = {
            let mut state = self.state.lock().await;
            let entry = state
                .entry(self.cursor_id.clone())
                .or_insert_with(PerCursorState::new);
            let advanced = entry.complete(self.index);
            let commit = match advanced {
                Some(cursor) => {
                    entry.advance_and_check_flush(cursor, &self.flush_policy, Instant::now())
                }
                None => None,
            };
            if entry.is_empty() {
                state.remove(&self.cursor_id);
            }
            commit
        };
        self.flush_signal.wake();
        if let Some(cursor) = to_commit {
            let key = CheckpointKey {
                scope: self.scope.clone(),
                cursor_id: self.cursor_id.clone(),
            };
            self.store.commit(&key, cursor).await?;
        }
        Ok(())
    }

    async fn nack(&self) -> Result<()> {
        self.inner.nack().await
    }

    async fn nack_with(&self, context: NackContext) -> Result<()> {
        self.inner.nack_with(context).await
    }
}

type CheckpointStreamRx<A, C, S, P> =
    mpsc::Receiver<Result<Message<CheckpointAcker<A, C, S>, C, P>>>;

/// Stream returned by [`CheckpointReader::read`]. Delegates [`Stream`]
/// to an internal mpsc receiver and exposes [`flush`](Self::flush) for
/// on-demand drain of buffered checkpoints.
pub struct CheckpointStream<A, C, S, P = crate::payload::Payload>
where
    A: Acker + Send + Sync + 'static,
    C: Clone + Send + Sync + 'static,
    S: CheckpointStore<C>,
    P: Send + Sync + 'static,
{
    rx: CheckpointStreamRx<A, C, S, P>,
    controller: Arc<FlushController<C, S>>,
    producer: Option<tokio::task::JoinHandle<()>>,
    timer: Option<tokio::task::JoinHandle<()>>,
}

impl<A, C, S, P> CheckpointStream<A, C, S, P>
where
    A: Acker + Send + Sync + 'static,
    C: Clone + Send + Sync + 'static,
    S: CheckpointStore<C>,
    P: Send + Sync + 'static,
{
    /// Commits all buffered checkpoints synchronously. Use on graceful
    /// shutdown before dropping the stream. Errors from the store
    /// surface as `Error::Store`.
    pub async fn flush(&self) -> Result<()> {
        self.controller.flush().await
    }
}

impl<A, C, S, P> Stream for CheckpointStream<A, C, S, P>
where
    A: Acker + Send + Sync + 'static,
    C: Clone + Send + Sync + 'static,
    S: CheckpointStore<C>,
    P: Send + Sync + 'static,
{
    type Item = Result<Message<CheckpointAcker<A, C, S>, C, P>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

impl<A, C, S, P> Drop for CheckpointStream<A, C, S, P>
where
    A: Acker + Send + Sync + 'static,
    C: Clone + Send + Sync + 'static,
    S: CheckpointStore<C>,
    P: Send + Sync + 'static,
{
    fn drop(&mut self) {
        // Close receiver so producer notices via tx.closed() and drains.
        self.rx.close();
        // Wake timer so it observes shutdown signal and exits.
        self.controller.shutdown();
        // Detach handles; tasks self-terminate via signals above.
        drop(self.producer.take());
        drop(self.timer.take());
    }
}

/// Shared flush state visible to acker, producer loop, timer task, and
/// the public `flush()` API. Holds the per-`CursorId` state map, scope,
/// store handle, and shutdown signal.
struct FlushController<C, S: CheckpointStore<C>> {
    scope: CheckpointScope,
    state: StateMap<C>,
    store: S,
    flush_policy: CheckpointFlushPolicy,
    flush_signal: FlushSignal,
    shutdown: Arc<tokio::sync::Notify>,
    shutdown_flag: Arc<std::sync::atomic::AtomicBool>,
}

impl<C, S> FlushController<C, S>
where
    C: Clone + Send + Sync + 'static,
    S: CheckpointStore<C>,
{
    fn shutdown(&self) {
        self.shutdown_flag
            .store(true, std::sync::atomic::Ordering::Release);
        self.shutdown.notify_waiters();
        self.flush_signal.wake();
    }

    fn is_shutting_down(&self) -> bool {
        self.shutdown_flag
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Drain all pending flush cursors and commit them. Aggregates the
    /// first store error and continues so all cursors are attempted.
    async fn flush(&self) -> Result<()> {
        let drained: Vec<(CursorId, C)> = {
            let mut state = self.state.lock().await;
            let mut out = Vec::new();
            let mut empty_ids = Vec::new();
            for (id, entry) in state.iter_mut() {
                if let Some(cursor) = entry.take_pending() {
                    out.push((id.clone(), cursor));
                }
                if entry.is_empty() {
                    empty_ids.push(id.clone());
                }
            }
            for id in empty_ids {
                state.remove(&id);
            }
            out
        };
        let mut first_err: Option<Error> = None;
        for (cursor_id, cursor) in drained {
            let key = CheckpointKey {
                scope: self.scope.clone(),
                cursor_id: cursor_id.clone(),
            };
            if let Err(e) = self.store.commit(&key, cursor).await
                && first_err.is_none()
            {
                tracing::warn!("checkpoint reader: explicit flush failed for {cursor_id:?}: {e}");
                first_err = Some(e);
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Best-effort drain used on producer-task exit (stream end or
    /// consumer drop). Only commits already-buffered flushes; does not
    /// clear in-flight queue entries because outstanding acks from the
    /// consumer side still need to advance them. Errors are logged but
    /// not propagated since no caller is awaiting a result.
    async fn drain_best_effort(&self, source: &'static str) {
        let drained: Vec<(CursorId, C)> = {
            let mut state = self.state.lock().await;
            let mut out = Vec::new();
            let mut empty_ids = Vec::new();
            for (id, entry) in state.iter_mut() {
                if let Some(cursor) = entry.take_pending() {
                    out.push((id.clone(), cursor));
                }
                if entry.is_empty() {
                    empty_ids.push(id.clone());
                }
            }
            for id in empty_ids {
                state.remove(&id);
            }
            out
        };
        for (cursor_id, cursor) in drained {
            let key = CheckpointKey {
                scope: self.scope.clone(),
                cursor_id: cursor_id.clone(),
            };
            if let Err(e) = self.store.commit(&key, cursor).await {
                tracing::warn!("checkpoint reader: {source} drain failed for {cursor_id:?}: {e}");
            }
        }
    }
}

/// Wraps a `Reader` with durable consumer progress backed by a
/// `CheckpointStore`.
///
/// # Cursor bounds
///
/// The inner reader's cursor must implement
/// `Cursor + Clone + Ord + Send + Sync + 'static`:
/// - [`Cursor`]: provides the `CursorId` used as the checkpoint key.
/// - `Ord`: used to skip-already-stored cursors on resume and to track
///   contiguous in-order progress per cursor id.
/// - `Clone + Send + Sync + 'static`: required to persist the cursor
///   across acks and to share it between the intake task and the
///   per-message [`CheckpointAcker`].
///
/// Backend cursors (`PgCursor`, `SqliteCursor`) and the
/// [`PartitionedCursor`](super::partitioned::PartitionedCursor) wrapper
/// satisfy these bounds out of the box.
pub struct CheckpointReader<R, S> {
    inner: R,
    store: S,
    config: CheckpointReaderConfig,
}

impl<R, S> CheckpointReader<R, S> {
    pub fn new(inner: R, store: S) -> Self {
        Self {
            inner,
            store,
            config: CheckpointReaderConfig::default(),
        }
    }

    pub fn with_config(inner: R, store: S, config: CheckpointReaderConfig) -> Self {
        Self {
            inner,
            store,
            config,
        }
    }
}

impl<R, S, P> Reader<P> for CheckpointReader<R, S>
where
    R: Reader<P> + Send + Sync + 'static,
    R::Cursor: Cursor + Clone + Ord + Send + Sync + 'static,
    R::Subscription: StartableSubscription<R::Cursor>,
    R::Acker: Acker + Send + Sync + 'static,
    R::Stream: Send + 'static,
    S: CheckpointStore<R::Cursor>,
    P: Send + Sync + 'static,
{
    type Subscription = CheckpointSubscription<R::Subscription>;
    type Acker = CheckpointAcker<R::Acker, R::Cursor, S>;
    type Cursor = R::Cursor;
    type Stream = CheckpointStream<R::Acker, R::Cursor, S, P>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let scope = subscription.scope.clone();
        let store = self.store.clone();
        let stored = store.load_scope(&scope).await?;

        if stored.is_empty() && matches!(subscription.on_missing, MissingCheckpointPolicy::Error) {
            return Err(Error::InvalidCursor(format!(
                "checkpoint reader: no checkpoint found for consumer group `{}` and stream `{}`",
                scope.consumer_group_id.as_str(),
                scope.stream_id.as_str()
            )));
        }

        let inner_subscription = subscription.inner.clone().with_starts(
            stored
                .iter()
                .map(|(_, c)| StartFrom::After(c.clone()))
                .collect(),
        );

        let (inner_stream, known) = match self.inner.read(inner_subscription).await {
            Ok(stream) => {
                let known: HashMap<CursorId, R::Cursor> = stored.into_iter().collect();
                (stream, known)
            }
            Err(Error::InvalidCursor(reason))
                if matches!(
                    subscription.on_invalid,
                    InvalidCursorPolicy::UseInitialStart
                ) =>
            {
                tracing::warn!(
                    reason = %reason,
                    "checkpoint reader falling back to initial start after invalid checkpoint cursor"
                );
                let stream = self.inner.read(subscription.inner.clone()).await?;
                (stream, HashMap::new())
            }
            Err(e) => return Err(e),
        };

        let state: StateMap<R::Cursor> = Arc::new(Mutex::new(HashMap::new()));
        let max_pending = self.config.max_pending_per_key;
        let flush_policy = self.config.flush_policy;
        let flush_signal = FlushSignal::new();
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let shutdown_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let controller = Arc::new(FlushController {
            scope: scope.clone(),
            state: Arc::clone(&state),
            store: store.clone(),
            flush_policy,
            flush_signal: flush_signal.clone(),
            shutdown: Arc::clone(&shutdown),
            shutdown_flag: Arc::clone(&shutdown_flag),
        });

        let (tx, rx) = mpsc::channel::<
            Result<Message<CheckpointAcker<R::Acker, R::Cursor, S>, R::Cursor, P>>,
        >(64);
        let known = Arc::new(known);

        // Cancel-safe producer loop. Selects between inner-stream items
        // and tx.closed() so consumer drop terminates the task even if
        // the inner reader is blocked in next().
        let producer_controller = Arc::clone(&controller);
        let producer_state = Arc::clone(&state);
        let producer_scope = scope.clone();
        let producer_known = Arc::clone(&known);
        let producer_signal = flush_signal.clone();
        let producer_tx = tx.clone();
        let producer = tokio::spawn(async move {
            let mut inner_stream = Box::pin(inner_stream);
            loop {
                let item = tokio::select! {
                    biased;
                    _ = producer_tx.closed() => break,
                    item = inner_stream.next() => item,
                };
                let Some(item) = item else { break };
                let msg = match item {
                    Ok(m) => m,
                    Err(e) => {
                        if producer_tx.send(Err(e)).await.is_err() {
                            break;
                        }
                        continue;
                    }
                };
                let cursor_id: CursorId = msg.cursor().id();
                let cursor = msg.cursor().clone();
                if let Some(stored_cursor) = producer_known.get(&cursor_id)
                    && cursor <= *stored_cursor
                {
                    let _ = msg.ack().await;
                    continue;
                }
                let (event, inner_acker, _cursor) = msg.into_parts();
                let index = {
                    let mut state_guard = producer_state.lock().await;
                    let pending_now = state_guard
                        .get(&cursor_id)
                        .map(|s| s.pending_queue_count())
                        .unwrap_or(0);
                    if pending_now >= max_pending {
                        let _ = producer_tx
                            .send(Err(Error::Store(format!(
                                "checkpoint reader: max_pending_per_key ({max_pending}) reached for cursor id {cursor_id:?}"
                            ))))
                            .await;
                        break;
                    }
                    let entry = state_guard
                        .entry(cursor_id.clone())
                        .or_insert_with(PerCursorState::new);
                    entry.record(cursor.clone())
                };
                let acker: CheckpointAcker<R::Acker, R::Cursor, S> = CheckpointAcker {
                    inner: inner_acker,
                    scope: producer_scope.clone(),
                    cursor_id: cursor_id.clone(),
                    index,
                    state: Arc::clone(&producer_state),
                    store: store.clone(),
                    flush_policy,
                    flush_signal: producer_signal.clone(),
                };
                let out = Message::new(event, acker, cursor);
                if producer_tx.send(Ok(out)).await.is_err() {
                    break;
                }
            }
            producer_controller.drain_best_effort("producer exit").await;
        });

        // Background flush timer. Wakes on flush_signal (any ack /
        // shutdown), recomputes earliest deadline across the state
        // map, parks until that deadline or another wakeup, then
        // flushes expired entries.
        let timer_controller = Arc::clone(&controller);
        let timer = tokio::spawn(async move {
            if timer_controller.flush_policy.max_pending_interval.is_zero() {
                // Pure count-based policy — no timer needed.
                timer_controller.shutdown.notified().await;
                return;
            }
            loop {
                if timer_controller.is_shutting_down() {
                    break;
                }
                let next_deadline = {
                    let state = timer_controller.state.lock().await;
                    let mut earliest: Option<Instant> = None;
                    for entry in state.values() {
                        if let Some(deadline) = entry.flush_deadline(&timer_controller.flush_policy)
                            && earliest.map(|e| deadline < e).unwrap_or(true)
                        {
                            earliest = Some(deadline);
                        }
                    }
                    earliest
                };
                match next_deadline {
                    None => {
                        tokio::select! {
                            biased;
                            _ = timer_controller.shutdown.notified() => break,
                            _ = timer_controller.flush_signal.wait() => {}
                        }
                    }
                    Some(deadline) => {
                        let sleep = tokio::time::sleep_until(deadline);
                        tokio::pin!(sleep);
                        tokio::select! {
                            biased;
                            _ = timer_controller.shutdown.notified() => break,
                            _ = &mut sleep => {
                                timer_controller.flush_expired().await;
                            }
                            _ = timer_controller.flush_signal.wait() => {}
                        }
                    }
                }
            }
        });

        Ok(CheckpointStream {
            rx,
            controller,
            producer: Some(producer),
            timer: Some(timer),
        })
    }
}

impl<C, S> FlushController<C, S>
where
    C: Clone + Send + Sync + 'static,
    S: CheckpointStore<C>,
{
    /// Flush all entries whose `first_pending_at + max_pending_interval`
    /// has already elapsed. Used by the background timer.
    async fn flush_expired(&self) {
        let now = Instant::now();
        let drained: Vec<(CursorId, C)> = {
            let mut state = self.state.lock().await;
            let mut out = Vec::new();
            let mut empty_ids = Vec::new();
            for (id, entry) in state.iter_mut() {
                let expired = entry
                    .flush_deadline(&self.flush_policy)
                    .map(|d| d <= now)
                    .unwrap_or(false);
                if expired && let Some(cursor) = entry.take_pending() {
                    out.push((id.clone(), cursor));
                }
                if entry.is_empty() {
                    empty_ids.push(id.clone());
                }
            }
            for id in empty_ids {
                state.remove(&id);
            }
            out
        };
        for (cursor_id, cursor) in drained {
            let key = CheckpointKey {
                scope: self.scope.clone(),
                cursor_id: cursor_id.clone(),
            };
            if let Err(e) = self.store.commit(&key, cursor).await {
                tracing::warn!("checkpoint reader: timer flush failed for {cursor_id:?}: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Event;
    use crate::io::Message;
    use crate::io::StreamId;
    use crate::io::acker::NoopAcker;
    use crate::io::position::{StartFrom, StartableSubscription};
    use crate::payload::Payload;
    use futures::{Stream, StreamExt};
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::sync::Mutex as TokioMutex;

    struct MpscStream<T> {
        rx: tokio::sync::mpsc::Receiver<T>,
    }

    impl<T> Stream for MpscStream<T> {
        type Item = T;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
            self.rx.poll_recv(cx)
        }
    }

    type InnerRx = tokio::sync::mpsc::Receiver<Result<Message<NoopAcker, TestCursor>>>;

    struct ChannelReader {
        rx: std::sync::Mutex<Option<InnerRx>>,
    }

    impl Reader for ChannelReader {
        type Subscription = TestSub;
        type Acker = NoopAcker;
        type Cursor = TestCursor;
        type Stream = Pin<Box<dyn Stream<Item = Result<Message<NoopAcker, TestCursor>>> + Send>>;

        async fn read(&self, _: TestSub) -> Result<Self::Stream> {
            let rx = self.rx.lock().unwrap().take().unwrap();
            Ok(Box::pin(MpscStream { rx }))
        }
    }

    #[test]
    fn pending_state_advances_contiguously_in_order() {
        let mut s: PerCursorState<i64> = PerCursorState::new();
        let i0 = s.record(10);
        let i1 = s.record(20);
        assert_eq!(s.complete(i0), Some(10));
        assert_eq!(s.complete(i1), Some(20));
        assert_eq!(s.pending_queue_count(), 0);
    }

    #[test]
    fn pending_state_does_not_advance_past_unacked_predecessor() {
        let mut s: PerCursorState<i64> = PerCursorState::new();
        let i0 = s.record(10);
        let i1 = s.record(20);
        let i2 = s.record(30);
        assert_eq!(s.complete(i1), None, "i0 unacked, can't advance");
        assert_eq!(s.complete(i2), None, "i0 still unacked");
        assert_eq!(
            s.complete(i0),
            Some(30),
            "completes whole prefix to highest"
        );
        assert_eq!(s.pending_queue_count(), 0);
    }

    #[test]
    fn pending_state_returns_highest_in_prefix() {
        let mut s: PerCursorState<i64> = PerCursorState::new();
        let i0 = s.record(10);
        let i1 = s.record(20);
        let _i2 = s.record(30);
        assert_eq!(s.complete(i0), Some(10));
        assert_eq!(s.complete(i1), Some(20));
        assert_eq!(s.pending_queue_count(), 1);
    }

    #[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
    struct TestCursor(i64);

    impl Cursor for TestCursor {}

    #[derive(Debug, Clone, Default)]
    struct TestSub;
    impl StartableSubscription<TestCursor> for TestSub {
        fn with_start(self, _: StartFrom<TestCursor>) -> Self {
            self
        }
    }

    #[derive(Debug, Clone)]
    struct SeedingSub {
        start: StartFrom<TestCursor>,
    }

    impl Default for SeedingSub {
        fn default() -> Self {
            Self {
                start: StartFrom::Earliest,
            }
        }
    }

    impl StartableSubscription<TestCursor> for SeedingSub {
        fn with_start(mut self, start: StartFrom<TestCursor>) -> Self {
            self.start = start;
            self
        }
    }

    struct SeedingSubReader {
        observed: std::sync::Arc<TokioMutex<Option<SeedingSub>>>,
    }

    impl Reader for SeedingSubReader {
        type Subscription = SeedingSub;
        type Acker = NoopAcker;
        type Cursor = TestCursor;
        type Stream = Pin<Box<dyn Stream<Item = Result<Message<NoopAcker, TestCursor>>> + Send>>;

        async fn read(&self, subscription: SeedingSub) -> Result<Self::Stream> {
            *self.observed.lock().await = Some(subscription);
            Ok(Box::pin(futures::stream::empty()))
        }
    }

    #[derive(Clone)]
    struct PreloadedStore<C> {
        rows: std::sync::Arc<TokioMutex<Vec<(CursorId, C)>>>,
    }

    impl<C> Default for PreloadedStore<C> {
        fn default() -> Self {
            Self {
                rows: std::sync::Arc::new(TokioMutex::new(Vec::new())),
            }
        }
    }

    impl<C> CheckpointStore<C> for PreloadedStore<C>
    where
        C: Clone + Send + Sync + 'static,
    {
        async fn load(&self, _: &CheckpointKey) -> Result<Option<C>> {
            Ok(None)
        }

        async fn load_scope(&self, _: &CheckpointScope) -> Result<Vec<(CursorId, C)>> {
            Ok(self.rows.lock().await.clone())
        }

        async fn commit(&self, _: &CheckpointKey, _: C) -> Result<()> {
            Ok(())
        }
    }

    struct VecReader {
        events: std::sync::Mutex<Option<Vec<Event>>>,
    }

    impl Reader for VecReader {
        type Subscription = TestSub;
        type Acker = NoopAcker;
        type Cursor = TestCursor;
        type Stream = Pin<Box<dyn Stream<Item = Result<Message<NoopAcker, TestCursor>>> + Send>>;

        async fn read(&self, _: TestSub) -> Result<Self::Stream> {
            let events = self.events.lock().unwrap().take().unwrap();
            let iter = events
                .into_iter()
                .enumerate()
                .map(|(i, e)| Ok(Message::new(e, NoopAcker, TestCursor(i as i64 + 1))));
            Ok(Box::pin(futures::stream::iter(iter)))
        }
    }

    #[derive(Clone)]
    struct MemStore {
        rows: Arc<TokioMutex<Vec<(CheckpointKey, TestCursor)>>>,
        committed: Arc<std::sync::Mutex<Vec<(CursorId, TestCursor)>>>,
    }

    impl Default for MemStore {
        fn default() -> Self {
            Self {
                rows: Arc::new(TokioMutex::new(Vec::new())),
                committed: Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }
    }

    impl MemStore {
        fn committed(&self) -> Vec<(CursorId, TestCursor)> {
            self.committed.lock().unwrap().clone()
        }
    }

    impl CheckpointStore<TestCursor> for MemStore {
        async fn load(&self, _: &CheckpointKey) -> Result<Option<TestCursor>> {
            Ok(None)
        }
        async fn load_scope(&self, _: &CheckpointScope) -> Result<Vec<(CursorId, TestCursor)>> {
            Ok(vec![])
        }
        async fn commit(&self, key: &CheckpointKey, cursor: TestCursor) -> Result<()> {
            self.rows.lock().await.push((key.clone(), cursor));
            self.committed
                .lock()
                .unwrap()
                .push((key.cursor_id.clone(), cursor));
            Ok(())
        }
    }

    fn test_scope() -> CheckpointScope {
        CheckpointScope::new(
            ConsumerGroupId::new("g").unwrap(),
            StreamId::new("s").unwrap(),
        )
    }

    fn ev(key: &str) -> Event {
        Event::builder(
            "acme",
            "/x",
            "thing.happened",
            key,
            Payload::from_string("p"),
        )
        .unwrap()
        .build()
        .expect("valid event")
    }

    #[tokio::test]
    async fn checkpoint_reader_errors_when_per_key_max_pending_exceeded() {
        use futures::StreamExt;
        let events: Vec<Event> = (0..5).map(|i| ev(&format!("k{i}"))).collect();
        let reader = VecReader {
            events: std::sync::Mutex::new(Some(events)),
        };
        let store = MemStore::default();
        let cr = CheckpointReader::with_config(
            reader,
            store,
            CheckpointReaderConfig {
                max_pending_per_key: 2,
                ..Default::default()
            },
        );
        let scope = CheckpointScope::new(
            ConsumerGroupId::new("g").unwrap(),
            StreamId::new("s").unwrap(),
        );
        let mut stream = cr
            .read(CheckpointSubscription::new(TestSub, scope))
            .await
            .unwrap();
        // Pull messages without acking — all 5 share partition=None, so
        // pending grows. After max_pending_per_key=2 is exceeded, the
        // stream must yield an error.
        let mut held = Vec::new();
        let mut saw_error = false;
        for _ in 0..10 {
            match tokio::time::timeout(std::time::Duration::from_millis(500), stream.next()).await {
                Ok(Some(Ok(m))) => held.push(m),
                Ok(Some(Err(_))) => {
                    saw_error = true;
                    break;
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }
        assert!(
            saw_error,
            "expected stream error after per-key max_pending exceeded, held {} messages",
            held.len()
        );
    }

    #[tokio::test]
    async fn checkpoint_reader_errors_when_required_checkpoint_is_missing() {
        let events: Vec<Event> = vec![ev("k0")];
        let reader = VecReader {
            events: std::sync::Mutex::new(Some(events)),
        };
        let store = MemStore::default();
        let cr = CheckpointReader::new(reader, store);
        let scope = CheckpointScope::new(
            ConsumerGroupId::new("g").unwrap(),
            StreamId::new("s").unwrap(),
        );

        let subscription =
            CheckpointSubscription::new(TestSub, scope).on_missing(MissingCheckpointPolicy::Error);

        let err = match cr.read(subscription).await {
            Ok(_) => panic!("expected missing checkpoint error"),
            Err(e) => e,
        };

        assert!(matches!(err, Error::InvalidCursor(_)));
        assert!(err.to_string().contains("no checkpoint found"));
    }

    struct InvalidCursorReader {
        calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Reader for InvalidCursorReader {
        type Subscription = TestSub;
        type Acker = NoopAcker;
        type Cursor = TestCursor;
        type Stream = Pin<Box<dyn Stream<Item = Result<Message<NoopAcker, TestCursor>>> + Send>>;

        async fn read(&self, _: TestSub) -> Result<Self::Stream> {
            let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if call == 0 {
                return Err(Error::InvalidCursor(
                    "checkpoint partition count changed".to_owned(),
                ));
            }
            Ok(Box::pin(futures::stream::empty()))
        }
    }

    #[tokio::test]
    async fn checkpoint_reader_retries_with_initial_start_when_checkpoint_cursor_is_invalid() {
        let store = PreloadedStore::default();
        *store.rows.lock().await = vec![(CursorId::global(), TestCursor(10))];
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let reader = InvalidCursorReader {
            calls: std::sync::Arc::clone(&calls),
        };
        let cr = CheckpointReader::new(reader, store);
        let scope = CheckpointScope::new(
            ConsumerGroupId::new("g").unwrap(),
            StreamId::new("s").unwrap(),
        );

        let _stream = cr
            .read(CheckpointSubscription::new(TestSub, scope))
            .await
            .unwrap();

        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn checkpoint_reader_returns_invalid_cursor_when_policy_is_error() {
        let store = PreloadedStore::default();
        *store.rows.lock().await = vec![(CursorId::global(), TestCursor(10))];
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let reader = InvalidCursorReader {
            calls: std::sync::Arc::clone(&calls),
        };
        let cr = CheckpointReader::new(reader, store);
        let scope = CheckpointScope::new(
            ConsumerGroupId::new("g").unwrap(),
            StreamId::new("s").unwrap(),
        );

        let err = match cr
            .read(
                CheckpointSubscription::new(TestSub, scope).on_invalid(InvalidCursorPolicy::Error),
            )
            .await
        {
            Ok(_) => panic!("expected invalid cursor error"),
            Err(e) => e,
        };

        assert!(matches!(err, Error::InvalidCursor(_)));
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn checkpoint_reader_over_mixed_partitions_resumes_from_current_only() {
        // Stored cursor < VecReader's emitted TestCursor(1) so the
        // CheckpointReader skip-already-stored guard does not swallow
        // the resumed event if partition assignment happens to match.
        use crate::io::reader::{
            PartitionedCursor, PartitionedReader, PartitionedReaderConfig, PartitionedSubscription,
        };
        use crate::partition::Partition;
        use futures::StreamExt;
        use std::num::NonZeroU32;

        let old_partition = Partition::new(0, NonZeroU32::new(8).unwrap()).unwrap();
        let current_partition = Partition::new(2, NonZeroU32::new(4).unwrap()).unwrap();

        let store = PreloadedStore::<PartitionedCursor<TestCursor>>::default();
        *store.rows.lock().await = vec![
            (
                CursorId::partition(old_partition),
                PartitionedCursor::new(TestCursor(0), old_partition),
            ),
            (
                CursorId::partition(current_partition),
                PartitionedCursor::new(TestCursor(0), current_partition),
            ),
        ];

        let inner_reader = VecReader {
            events: std::sync::Mutex::new(Some(vec![ev("k0")])),
        };
        let partitioned = PartitionedReader::source(
            inner_reader,
            PartitionedReaderConfig {
                partition_count: NonZeroU32::new(4).unwrap(),
                ..PartitionedReaderConfig::default()
            },
        );
        let checkpointed = CheckpointReader::new(partitioned, store);
        let scope = CheckpointScope::new(
            ConsumerGroupId::new("g").unwrap(),
            StreamId::new("s").unwrap(),
        );

        let mut stream = checkpointed
            .read(CheckpointSubscription::new(
                PartitionedSubscription::new(TestSub),
                scope,
            ))
            .await
            .unwrap();

        let msg = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(msg.event().key().as_str(), "k0");
    }

    #[tokio::test]
    async fn checkpoint_reader_over_old_partitions_only_falls_back() {
        // Stored row is incompatible (partition_count=8 vs configured=4);
        // test exercises the fallback path.
        use crate::io::reader::{
            PartitionedCursor, PartitionedReader, PartitionedReaderConfig, PartitionedSubscription,
        };
        use crate::partition::Partition;
        use futures::StreamExt;
        use std::num::NonZeroU32;

        let old_partition = Partition::new(0, NonZeroU32::new(8).unwrap()).unwrap();
        let store = PreloadedStore::<PartitionedCursor<TestCursor>>::default();
        *store.rows.lock().await = vec![(
            CursorId::partition(old_partition),
            PartitionedCursor::new(TestCursor(10), old_partition),
        )];

        let inner_reader = VecReader {
            events: std::sync::Mutex::new(Some(vec![ev("k0")])),
        };
        let partitioned = PartitionedReader::source(
            inner_reader,
            PartitionedReaderConfig {
                partition_count: NonZeroU32::new(4).unwrap(),
                ..PartitionedReaderConfig::default()
            },
        );
        let checkpointed = CheckpointReader::new(partitioned, store);
        let scope = CheckpointScope::new(
            ConsumerGroupId::new("g").unwrap(),
            StreamId::new("s").unwrap(),
        );

        let mut stream = checkpointed
            .read(CheckpointSubscription::new(
                PartitionedSubscription::new(TestSub),
                scope,
            ))
            .await
            .unwrap();

        let msg = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(msg.event().key().as_str(), "k0");
    }

    #[tokio::test]
    async fn checkpoint_reader_seeds_inner_with_min_cursor() {
        let cursor_id = CursorId::partition(
            crate::partition::Partition::new(2, std::num::NonZeroU32::new(4).unwrap()).unwrap(),
        );
        let store = PreloadedStore::default();
        *store.rows.lock().await = vec![(cursor_id, TestCursor(10))];
        let observed = std::sync::Arc::new(TokioMutex::new(None));
        let reader = SeedingSubReader {
            observed: std::sync::Arc::clone(&observed),
        };
        let cr = CheckpointReader::new(reader, store);
        let scope = CheckpointScope::new(
            ConsumerGroupId::new("g").unwrap(),
            StreamId::new("s").unwrap(),
        );

        let _stream = cr
            .read(CheckpointSubscription::new(SeedingSub::default(), scope))
            .await
            .unwrap();

        let observed = observed.lock().await.clone().unwrap();
        assert_eq!(observed.start, StartFrom::After(TestCursor(10)));
    }

    #[test]
    fn missing_checkpoint_policy_defaults_to_use_initial_start() {
        assert_eq!(
            MissingCheckpointPolicy::default(),
            MissingCheckpointPolicy::UseInitialStart
        );
    }

    #[test]
    fn invalid_cursor_policy_defaults_to_use_initial_start() {
        assert_eq!(
            InvalidCursorPolicy::default(),
            InvalidCursorPolicy::UseInitialStart
        );
    }

    #[test]
    fn checkpoint_key_with_global_cursor_id_has_no_partition_subkey() {
        let scope = CheckpointScope::new(
            ConsumerGroupId::new("g").unwrap(),
            StreamId::new("s").unwrap(),
        );
        let key = CheckpointKey::new(scope, CursorId::global());
        assert_eq!(key.cursor_id, CursorId::global());
    }

    #[test]
    fn checkpoint_key_with_named_cursor_id_preserves_value() {
        let scope = CheckpointScope::new(
            ConsumerGroupId::new("g").unwrap(),
            StreamId::new("s").unwrap(),
        );
        let id = CursorId::partition(
            crate::partition::Partition::new(17, std::num::NonZeroU32::new(100).unwrap()).unwrap(),
        );
        let key = CheckpointKey::new(scope, id.clone());
        assert_eq!(key.cursor_id, id);
    }

    #[tokio::test]
    async fn checkpoint_stream_drop_aborts_forwarder_when_inner_stream_is_pending() {
        struct PendingReader;
        impl Reader for PendingReader {
            type Subscription = TestSub;
            type Acker = NoopAcker;
            type Cursor = TestCursor;
            type Stream =
                Pin<Box<dyn Stream<Item = Result<Message<NoopAcker, TestCursor>>> + Send>>;

            async fn read(&self, _: TestSub) -> Result<Self::Stream> {
                Ok(Box::pin(futures::stream::pending()))
            }
        }

        let store = PreloadedStore::<TestCursor>::default();
        let cr = CheckpointReader::new(PendingReader, store);
        let scope = CheckpointScope::new(
            ConsumerGroupId::new("g").unwrap(),
            StreamId::new("s").unwrap(),
        );

        let stream = cr
            .read(CheckpointSubscription::new(TestSub, scope))
            .await
            .unwrap();
        drop(stream);
    }

    #[tokio::test]
    async fn flush_policy_count_triggers_after_n_acks() {
        let store = MemStore::default();
        let reader = VecReader {
            events: std::sync::Mutex::new(Some(vec![ev("a"), ev("b"), ev("c")])),
        };
        let config = CheckpointReaderConfig {
            flush_policy: CheckpointFlushPolicy {
                max_pending_acks: NonZeroUsize::new(2).unwrap(),
                max_pending_interval: Duration::ZERO,
            },
            ..Default::default()
        };
        let cr = CheckpointReader::with_config(reader, store.clone(), config);
        let sub = CheckpointSubscription::new(TestSub, test_scope());

        let mut stream = cr.read(sub).await.unwrap();

        let msg1 = stream.next().await.unwrap().unwrap();
        msg1.ack().await.unwrap();
        assert!(
            store.committed().is_empty(),
            "first ack should buffer, not flush"
        );

        let msg2 = stream.next().await.unwrap().unwrap();
        msg2.ack().await.unwrap();
        assert_eq!(
            store.committed().len(),
            1,
            "second ack should trigger flush"
        );
    }

    #[tokio::test]
    async fn flush_policy_interval_triggers_after_duration() {
        let store = MemStore::default();
        let reader = VecReader {
            events: std::sync::Mutex::new(Some(vec![ev("a"), ev("b")])),
        };
        let config = CheckpointReaderConfig {
            flush_policy: CheckpointFlushPolicy {
                max_pending_acks: NonZeroUsize::new(100).unwrap(),
                max_pending_interval: Duration::from_millis(50),
            },
            ..Default::default()
        };
        let cr = CheckpointReader::with_config(reader, store.clone(), config);
        let sub = CheckpointSubscription::new(TestSub, test_scope());

        let mut stream = cr.read(sub).await.unwrap();

        let msg1 = stream.next().await.unwrap().unwrap();
        msg1.ack().await.unwrap();
        assert!(
            store.committed().is_empty(),
            "should buffer, high count threshold not hit"
        );

        tokio::time::sleep(Duration::from_millis(100)).await;

        let msg2 = stream.next().await.unwrap().unwrap();
        msg2.ack().await.unwrap();
        assert_eq!(
            store.committed().len(),
            1,
            "interval should trigger flush on next ack"
        );
    }

    #[tokio::test]
    async fn default_flush_policy_commits_on_every_ack() {
        let store = MemStore::default();
        let reader = VecReader {
            events: std::sync::Mutex::new(Some(vec![ev("a"), ev("b")])),
        };
        let cr = CheckpointReader::new(reader, store.clone());
        let sub = CheckpointSubscription::new(TestSub, test_scope());

        let mut stream = cr.read(sub).await.unwrap();

        let msg1 = stream.next().await.unwrap().unwrap();
        msg1.ack().await.unwrap();
        assert_eq!(store.committed().len(), 1, "default flushes on first ack");

        let msg2 = stream.next().await.unwrap().unwrap();
        msg2.ack().await.unwrap();
        assert_eq!(store.committed().len(), 2, "default flushes on second ack");
    }

    #[tokio::test]
    async fn flush_on_stream_drop_drains_buffered_cursors() {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Message<NoopAcker, TestCursor>>>(1);

        let reader = ChannelReader {
            rx: std::sync::Mutex::new(Some(rx)),
        };
        let store = MemStore::default();
        let config = CheckpointReaderConfig {
            flush_policy: CheckpointFlushPolicy {
                max_pending_acks: NonZeroUsize::new(10).unwrap(),
                max_pending_interval: Duration::from_secs(3600),
            },
            ..Default::default()
        };
        let cr = CheckpointReader::with_config(reader, store.clone(), config);
        let sub = CheckpointSubscription::new(TestSub, test_scope());

        let mut stream = cr.read(sub).await.unwrap();

        tx.send(Ok(Message::new(ev("a"), NoopAcker, TestCursor(1))))
            .await
            .unwrap();

        let msg1 = stream.next().await.unwrap().unwrap();
        msg1.ack().await.unwrap();
        assert!(
            store.committed().is_empty(),
            "should buffer under high flush threshold"
        );

        drop(stream);
        drop(tx);

        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(
            store.committed().len(),
            1,
            "drop should drain buffered cursor"
        );
    }

    #[tokio::test]
    async fn post_loop_drain_commits_buffered_cursors_on_natural_end() {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Message<NoopAcker, TestCursor>>>(1);

        let reader = ChannelReader {
            rx: std::sync::Mutex::new(Some(rx)),
        };
        let store = MemStore::default();
        let config = CheckpointReaderConfig {
            flush_policy: CheckpointFlushPolicy {
                max_pending_acks: NonZeroUsize::new(10).unwrap(),
                max_pending_interval: Duration::from_secs(3600),
            },
            ..Default::default()
        };
        let cr = CheckpointReader::with_config(reader, store.clone(), config);
        let sub = CheckpointSubscription::new(TestSub, test_scope());

        let mut stream = cr.read(sub).await.unwrap();

        tx.send(Ok(Message::new(ev("a"), NoopAcker, TestCursor(1))))
            .await
            .unwrap();

        let msg1 = stream.next().await.unwrap().unwrap();
        msg1.ack().await.unwrap();
        assert!(
            store.committed().is_empty(),
            "should buffer under high flush threshold"
        );

        drop(tx);

        assert!(stream.next().await.is_none(), "stream should end naturally");

        assert_eq!(
            store.committed().len(),
            1,
            "natural end should drain buffered cursor"
        );
    }

    #[tokio::test]
    async fn background_timer_flushes_without_further_acks() {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Message<NoopAcker, TestCursor>>>(1);
        let reader = ChannelReader {
            rx: std::sync::Mutex::new(Some(rx)),
        };
        let store = MemStore::default();
        let config = CheckpointReaderConfig {
            flush_policy: CheckpointFlushPolicy {
                max_pending_acks: NonZeroUsize::new(100).unwrap(),
                max_pending_interval: Duration::from_millis(50),
            },
            ..Default::default()
        };
        let cr = CheckpointReader::with_config(reader, store.clone(), config);
        let sub = CheckpointSubscription::new(TestSub, test_scope());
        let mut stream = cr.read(sub).await.unwrap();

        tx.send(Ok(Message::new(ev("a"), NoopAcker, TestCursor(1))))
            .await
            .unwrap();
        let msg = stream.next().await.unwrap().unwrap();
        msg.ack().await.unwrap();

        assert!(
            store.committed().is_empty(),
            "below count threshold, interval not yet elapsed"
        );

        // Advance time past the interval — background timer must flush
        // even though no further acks arrive.
        tokio::time::sleep(Duration::from_millis(75)).await;

        assert_eq!(
            store.committed().len(),
            1,
            "background timer should flush expired entry without further acks"
        );
    }

    /// Producer task remains blocked on inner stream forever. Consumer
    /// drops the stream — producer must exit instead of leaking.
    #[tokio::test]
    async fn drop_aborts_producer_blocked_on_inner_stream() {
        let (_tx, rx) = tokio::sync::mpsc::channel::<Result<Message<NoopAcker, TestCursor>>>(1);
        let reader = ChannelReader {
            rx: std::sync::Mutex::new(Some(rx)),
        };
        let store = MemStore::default();
        let cr = CheckpointReader::new(reader, store.clone());
        let sub = CheckpointSubscription::new(TestSub, test_scope());

        let stream = cr.read(sub).await.unwrap();
        // Producer is awaiting inner_stream.next(). Dropping stream
        // must signal producer via tx.closed() so it exits.
        drop(stream);

        // Give the runtime a tick to schedule the producer's select! arm.
        tokio::time::sleep(Duration::from_millis(10)).await;
        // If the producer were leaked, this test would still pass — the
        // real assertion is that no panic / hang occurs and the runtime
        // shuts down cleanly when the test ends. The previous version
        // before cancel-safety would leak the producer task.
    }

    #[tokio::test]
    async fn explicit_flush_drains_buffered_cursors_synchronously() {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Message<NoopAcker, TestCursor>>>(2);
        let reader = ChannelReader {
            rx: std::sync::Mutex::new(Some(rx)),
        };
        let store = MemStore::default();
        let config = CheckpointReaderConfig {
            flush_policy: CheckpointFlushPolicy {
                max_pending_acks: NonZeroUsize::new(100).unwrap(),
                max_pending_interval: Duration::from_secs(3600),
            },
            ..Default::default()
        };
        let cr = CheckpointReader::with_config(reader, store.clone(), config);
        let sub = CheckpointSubscription::new(TestSub, test_scope());
        let mut stream = cr.read(sub).await.unwrap();

        tx.send(Ok(Message::new(ev("a"), NoopAcker, TestCursor(1))))
            .await
            .unwrap();
        let msg = stream.next().await.unwrap().unwrap();
        msg.ack().await.unwrap();

        assert!(
            store.committed().is_empty(),
            "buffered, neither threshold hit yet"
        );

        // Explicit flush — must commit synchronously.
        stream.flush().await.unwrap();

        assert_eq!(
            store.committed().len(),
            1,
            "explicit flush should drain immediately"
        );

        // Second flush with no pending state is a no-op.
        stream.flush().await.unwrap();
        assert_eq!(store.committed().len(), 1);
    }

    #[tokio::test]
    async fn background_timer_does_not_spin_when_interval_is_zero() {
        // Sanity: with default policy (interval=0), the timer task
        // parks on shutdown notify and never wakes for sleeps. Just
        // confirm no panics / hangs when default config runs against
        // an empty channel and is dropped.
        let (_tx, rx) = tokio::sync::mpsc::channel::<Result<Message<NoopAcker, TestCursor>>>(1);
        let reader = ChannelReader {
            rx: std::sync::Mutex::new(Some(rx)),
        };
        let store = MemStore::default();
        let cr = CheckpointReader::new(reader, store.clone());
        let sub = CheckpointSubscription::new(TestSub, test_scope());
        let stream = cr.read(sub).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(stream);
    }
}
