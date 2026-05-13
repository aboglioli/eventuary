//! CheckpointReader: composes an inner reader and a checkpoint store.
//!
//! On `read`, loads the persisted cursor for the requested scope and
//! configures the inner reader to start from `StartFrom::After(cursor)`.
//! Downstream messages get a `CheckpointAcker` that commits the cursor on
//! `ack` (contiguous delivered order per key), no-op on `nack`.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{Stream, StreamExt};
use tokio::sync::Mutex;
use tokio::sync::mpsc;

use crate::error::{Error, Result};
use crate::io::checkpoint::{CheckpointKey, CheckpointScope, CheckpointStore};
use crate::io::{Acker, Message, Reader};
use crate::partition::{CursorPartition, LogicalPartition};
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

pub struct CheckpointAcker<A: Acker, C> {
    inner: A,
    scope: CheckpointScope,
    partition: Option<LogicalPartition>,
    index: usize,
    state: Arc<Mutex<HashMap<Option<LogicalPartition>, PendingState<C>>>>,
    commit_tx: mpsc::Sender<(CheckpointKey, C)>,
}

impl<A, C> Acker for CheckpointAcker<A, C>
where
    A: Acker + Send + Sync + 'static,
    C: Clone + Send + Sync + 'static,
{
    async fn ack(&self) -> Result<()> {
        self.inner.ack().await?;
        let mut state = self.state.lock().await;
        let entry = state
            .entry(self.partition)
            .or_insert_with(PendingState::new);
        if let Some(cursor) = entry.complete(self.index) {
            drop(state);
            let key = CheckpointKey {
                scope: self.scope.clone(),
                partition: self.partition,
            };
            let _ = self.commit_tx.send((key, cursor)).await;
        }
        Ok(())
    }

    async fn nack(&self) -> Result<()> {
        self.inner.nack().await
    }
}

pub struct CheckpointStream<A: Acker + 'static, C: Clone + Send + Sync + 'static> {
    rx: mpsc::Receiver<Result<Message<CheckpointAcker<A, C>, C>>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl<A: Acker + 'static, C: Clone + Send + Sync + 'static> Drop for CheckpointStream<A, C> {
    fn drop(&mut self) {
        if let Some(h) = self.handle.take() {
            h.abort();
        }
    }
}

impl<A: Acker + 'static, C: Clone + Send + Sync + 'static> Stream for CheckpointStream<A, C> {
    type Item = Result<Message<CheckpointAcker<A, C>, C>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

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
    R::Subscription: StartableSubscription<R::Cursor>,
    R::Cursor: Clone + Ord + CursorPartition + Send + Sync + 'static,
    R::Acker: Acker + Send + Sync + 'static,
    R::Stream: Send + 'static,
    S: CheckpointStore<R::Cursor>,
{
    type Subscription = CheckpointSubscription<R::Subscription>;
    type Acker = CheckpointAcker<R::Acker, R::Cursor>;
    type Cursor = R::Cursor;
    type Stream = CheckpointStream<R::Acker, R::Cursor>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let scope = subscription.scope.clone();
        let store = self.store.clone();
        let stored = store.load_scope(&scope).await?;
        let known: HashMap<Option<LogicalPartition>, R::Cursor> = stored.into_iter().collect();
        let min_cursor: Option<R::Cursor> = known.values().min().cloned();

        let inner_subscription = match min_cursor {
            Some(cursor) => subscription.inner.with_start(StartFrom::After(cursor)),
            None => subscription.inner,
        };

        let inner_stream = self.inner.read(inner_subscription).await?;
        type State<C> = Arc<Mutex<HashMap<Option<LogicalPartition>, PendingState<C>>>>;
        let state: State<R::Cursor> = Arc::new(Mutex::new(HashMap::new()));
        let (tx, rx) =
            mpsc::channel::<Result<Message<CheckpointAcker<R::Acker, R::Cursor>, R::Cursor>>>(64);
        let (commit_tx, mut commit_rx) = mpsc::channel::<(CheckpointKey, R::Cursor)>(64);
        let max_pending = self.config.max_pending_per_key;

        let commit_store = store.clone();
        let commit_task = tokio::spawn(async move {
            while let Some((key, cursor)) = commit_rx.recv().await {
                if let Err(e) = commit_store.commit(&key, cursor).await {
                    tracing::warn!("checkpoint commit failed: {e}");
                }
            }
        });

        let state_for_stream = Arc::clone(&state);
        let scope_for_stream = scope.clone();
        let stream_handle = tokio::spawn(async move {
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
                let cursor = msg.cursor().clone();
                if let Some(stored_cursor) = known.get(&cursor_partition)
                    && cursor <= *stored_cursor
                {
                    let _ = msg.ack().await;
                    continue;
                }
                let (event, inner_acker, inner_cursor) = msg.into_parts();
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
                                .send(Err(Error::Store(
                                    "checkpoint reader: all partitions hit max_pending_per_key"
                                        .to_owned(),
                                )))
                                .await;
                            return;
                        } else {
                            tracing::warn!(
                                "checkpoint reader partition pending limit reached, waiting"
                            );
                        }
                    }
                    let entry = state_guard
                        .entry(cursor_partition)
                        .or_insert_with(PendingState::new);
                    entry.record(inner_cursor.clone())
                };
                let acker = CheckpointAcker {
                    inner: inner_acker,
                    scope: scope_for_stream.clone(),
                    partition: cursor_partition,
                    index,
                    state: Arc::clone(&state_for_stream),
                    commit_tx: commit_tx.clone(),
                };
                let out = Message::new(event, acker, inner_cursor);
                if tx.send(Ok(out)).await.is_err() {
                    return;
                }
            }
        });

        let handle = tokio::spawn(async move {
            let _ = stream_handle.await;
            let _ = commit_task.await;
        });

        Ok(CheckpointStream {
            rx,
            handle: Some(handle),
        })
    }
}

#[cfg(test)]
mod tests {
    // Tests live in Task 11 composition tests (eventuary-sqlite + eventuary-postgres).
}
