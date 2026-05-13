//! CheckpointReader: composes an inner reader and a checkpoint store.
//!
//! On `read`, loads the persisted cursor per partition for the requested
//! scope and configures the inner reader to start from
//! `StartFrom::After(min(stored_cursors))`. Records each delivered cursor
//! in contiguous delivered order per partition. On downstream `ack`,
//! calls the inner ack first, then commits the cursor to the store
//! **synchronously** — store commit errors propagate to the caller and
//! to consumers of the stream.
//!
//! The acker holds the cursor type the store persists: `Commit`. The
//! delivery cursor is `R::Cursor`; `CommitCursor::commit_cursor()`
//! produces the value the store sees. For partitioned composition
//! (`PartitionedReader<R>`) the delivery cursor is
//! `PartitionedCursor<R::Cursor>` and its `commit_cursor()` strips the
//! partition envelope.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use futures::{Stream, StreamExt};
use tokio::sync::Mutex;
use tokio::sync::mpsc;

use crate::error::{Error, Result};
use crate::io::checkpoint::{CheckpointKey, CheckpointScope, CheckpointStore};
use crate::io::{Acker, Message, Reader};
use crate::partition::{CommitCursor, CursorPartition, LogicalPartition};
use crate::start_from::{StartFrom, StartableSubscription};

#[derive(Debug, Clone)]
pub struct CheckpointReaderConfig {
    pub max_pending_per_key: usize,
}

impl Default for CheckpointReaderConfig {
    fn default() -> Self {
        Self {
            max_pending_per_key: 1024,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CheckpointSubscription<S> {
    pub inner: S,
    pub scope: CheckpointScope,
}

impl<S> CheckpointSubscription<S> {
    pub fn new(inner: S, scope: CheckpointScope) -> Self {
        Self { inner, scope }
    }
}

struct PendingState<C> {
    delivered: Vec<C>,
    completed: std::collections::HashSet<usize>,
    next_to_commit: usize,
}

impl<C: Clone> PendingState<C> {
    fn new() -> Self {
        Self {
            delivered: Vec::new(),
            completed: std::collections::HashSet::new(),
            next_to_commit: 0,
        }
    }

    fn record(&mut self, cursor: C) -> usize {
        let idx = self.delivered.len();
        self.delivered.push(cursor);
        idx
    }

    fn complete(&mut self, idx: usize) -> Option<C> {
        self.completed.insert(idx);
        let mut advanced: Option<C> = None;
        while self.completed.remove(&self.next_to_commit) {
            advanced = Some(self.delivered[self.next_to_commit].clone());
            self.next_to_commit += 1;
        }
        advanced
    }

    fn pending_count(&self) -> usize {
        self.delivered.len() - self.next_to_commit
    }
}

type PendingMap<C> = Arc<Mutex<HashMap<Option<LogicalPartition>, PendingState<C>>>>;

pub struct CheckpointAcker<A: Acker, C, S: CheckpointStore<C>> {
    inner: A,
    scope: CheckpointScope,
    partition: Option<LogicalPartition>,
    index: usize,
    state: PendingMap<C>,
    store: S,
}

impl<A, C, S> Acker for CheckpointAcker<A, C, S>
where
    A: Acker + Send + Sync + 'static,
    C: Clone + Send + Sync + 'static,
    S: CheckpointStore<C>,
{
    async fn ack(&self) -> Result<()> {
        self.inner.ack().await?;
        let advanced = {
            let mut state = self.state.lock().await;
            let entry = state
                .entry(self.partition)
                .or_insert_with(PendingState::new);
            entry.complete(self.index)
        };
        if let Some(cursor) = advanced {
            let key = CheckpointKey {
                scope: self.scope.clone(),
                partition: self.partition,
            };
            self.store.commit(&key, cursor).await?;
        }
        Ok(())
    }

    async fn nack(&self) -> Result<()> {
        self.inner.nack().await
    }
}

pub type CheckpointStream<A, C, S> =
    Pin<Box<dyn Stream<Item = Result<Message<CheckpointAcker<A, C, S>, C>>> + Send>>;

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

impl<R, S> Reader for CheckpointReader<R, S>
where
    R: Reader + Send + Sync + 'static,
    R::Cursor: CommitCursor + CursorPartition + Send + Sync + 'static,
    <R::Cursor as CommitCursor>::Commit: Send + Sync + 'static,
    R::Subscription: StartableSubscription<<R::Cursor as CommitCursor>::Commit>,
    R::Acker: Acker + Send + Sync + 'static,
    R::Stream: Send + 'static,
    S: CheckpointStore<<R::Cursor as CommitCursor>::Commit>,
{
    type Subscription = CheckpointSubscription<R::Subscription>;
    type Acker = CheckpointAcker<R::Acker, <R::Cursor as CommitCursor>::Commit, S>;
    type Cursor = <R::Cursor as CommitCursor>::Commit;
    type Stream = CheckpointStream<R::Acker, <R::Cursor as CommitCursor>::Commit, S>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let scope = subscription.scope.clone();
        let store = self.store.clone();
        let stored = store.load_scope(&scope).await?;
        let known: HashMap<Option<LogicalPartition>, <R::Cursor as CommitCursor>::Commit> =
            stored.into_iter().collect();
        let min_cursor: Option<<R::Cursor as CommitCursor>::Commit> = known.values().min().cloned();

        let inner_subscription = match min_cursor {
            Some(cursor) => subscription.inner.with_start(StartFrom::After(cursor)),
            None => subscription.inner,
        };

        let inner_stream = self.inner.read(inner_subscription).await?;
        let state: PendingMap<<R::Cursor as CommitCursor>::Commit> =
            Arc::new(Mutex::new(HashMap::new()));
        let max_pending = self.config.max_pending_per_key;
        let (tx, rx) = mpsc::channel::<
            Result<
                Message<
                    CheckpointAcker<R::Acker, <R::Cursor as CommitCursor>::Commit, S>,
                    <R::Cursor as CommitCursor>::Commit,
                >,
            >,
        >(64);
        let known = Arc::new(known);

        let state_for_stream = Arc::clone(&state);
        let scope_for_stream = scope.clone();
        let known_for_stream = Arc::clone(&known);
        let store_for_stream = store.clone();
        let handle = tokio::spawn(async move {
            let mut inner_stream = Box::pin(inner_stream);
            while let Some(item) = inner_stream.next().await {
                let msg = match item {
                    Ok(m) => m,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        continue;
                    }
                };
                let cursor_partition = msg.cursor().partition();
                let commit = msg.cursor().commit_cursor();
                if let Some(stored_cursor) = known_for_stream.get(&cursor_partition)
                    && commit <= *stored_cursor
                {
                    let _ = msg.ack().await;
                    continue;
                }
                let (event, inner_acker, _cursor) = msg.into_parts();
                let index = {
                    let mut state_guard = state_for_stream.lock().await;
                    let pending_now = state_guard
                        .get(&cursor_partition)
                        .map(|s| s.pending_count())
                        .unwrap_or(0);
                    if pending_now >= max_pending {
                        let all_jammed = state_guard
                            .values()
                            .all(|s| s.pending_count() >= max_pending);
                        if all_jammed {
                            let _ = tx
                                .send(Err(Error::Store(format!(
                                    "checkpoint reader: all partitions hit max_pending_per_key ({max_pending})"
                                ))))
                                .await;
                            return;
                        }
                        tracing::warn!(
                            partition = ?cursor_partition,
                            pending = pending_now,
                            max = max_pending,
                            "checkpoint reader partition at max_pending_per_key, continuing other lanes"
                        );
                    }
                    let entry = state_guard
                        .entry(cursor_partition)
                        .or_insert_with(PendingState::new);
                    entry.record(commit.clone())
                };
                let acker: CheckpointAcker<R::Acker, <R::Cursor as CommitCursor>::Commit, S> =
                    CheckpointAcker {
                        inner: inner_acker,
                        scope: scope_for_stream.clone(),
                        partition: cursor_partition,
                        index,
                        state: Arc::clone(&state_for_stream),
                        store: store_for_stream.clone(),
                    };
                let out = Message::new(event, acker, commit);
                if tx.send(Ok(out)).await.is_err() {
                    return;
                }
            }
        });

        let stream = futures::stream::unfold((rx, Some(handle)), |(mut rx, handle)| async move {
            rx.recv().await.map(|item| (item, (rx, handle)))
        });
        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::PendingState;

    #[test]
    fn pending_state_advances_contiguously_in_order() {
        let mut s: PendingState<i64> = PendingState::new();
        let i0 = s.record(10);
        let i1 = s.record(20);
        assert_eq!(s.complete(i0), Some(10));
        assert_eq!(s.complete(i1), Some(20));
        assert_eq!(s.pending_count(), 0);
    }

    #[test]
    fn pending_state_does_not_advance_past_unacked_predecessor() {
        let mut s: PendingState<i64> = PendingState::new();
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
        assert_eq!(s.pending_count(), 0);
    }

    #[test]
    fn pending_state_returns_highest_in_prefix() {
        let mut s: PendingState<i64> = PendingState::new();
        let i0 = s.record(10);
        let i1 = s.record(20);
        let _i2 = s.record(30);
        assert_eq!(s.complete(i0), Some(10));
        assert_eq!(s.complete(i1), Some(20));
        assert_eq!(s.pending_count(), 1);
    }
}
