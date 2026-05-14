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
use crate::io::checkpoint::{
    CheckpointKey, CheckpointResumableSubscription, CheckpointResume, CheckpointResumePoint,
    CheckpointResumePolicy, CheckpointScope, CheckpointStore,
};
use crate::io::{Acker, Message, Reader};
use crate::partition::{CommitCursor, CursorPartition, LogicalPartition};
use crate::start_from::StartFrom;

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
    pub resume_policy: CheckpointResumePolicy,
}

impl<S> CheckpointSubscription<S> {
    pub fn new(inner: S, scope: CheckpointScope) -> Self {
        Self {
            inner,
            scope,
            resume_policy: CheckpointResumePolicy::UseInitialStart,
        }
    }

    pub fn with_resume_policy(mut self, policy: CheckpointResumePolicy) -> Self {
        self.resume_policy = policy;
        self
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
    R::Subscription: CheckpointResumableSubscription<<R::Cursor as CommitCursor>::Commit>,
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

        if stored.is_empty() && matches!(subscription.resume_policy, CheckpointResumePolicy::Error)
        {
            return Err(Error::InvalidCursor(format!(
                "checkpoint reader: no checkpoint found for consumer group `{}` and stream `{}`",
                scope.consumer_group_id.as_str(),
                scope.stream_id.as_str()
            )));
        }

        let resume_points = stored
            .iter()
            .cloned()
            .map(|(partition, cursor)| CheckpointResumePoint::new(partition, cursor))
            .collect::<Vec<_>>();
        let known_from_checkpoints: HashMap<
            Option<LogicalPartition>,
            <R::Cursor as CommitCursor>::Commit,
        > = stored.into_iter().collect();
        let min_cursor: Option<<R::Cursor as CommitCursor>::Commit> =
            known_from_checkpoints.values().min().cloned();

        let inner_subscription = match min_cursor {
            Some(cursor) => {
                subscription
                    .inner
                    .clone()
                    .with_checkpoint_resume(CheckpointResume::new(
                        StartFrom::After(cursor),
                        resume_points,
                    ))
            }
            None => subscription.inner.clone(),
        };

        let (inner_stream, known) = match self.inner.read(inner_subscription).await {
            Ok(stream) => (stream, known_from_checkpoints),
            Err(Error::InvalidCursor(reason))
                if matches!(
                    subscription.resume_policy,
                    CheckpointResumePolicy::UseInitialStart
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
                        let _ = tx
                            .send(Err(Error::Store(format!(
                                "checkpoint reader: max_pending_per_key ({max_pending}) reached for partition {cursor_partition:?}"
                            ))))
                            .await;
                        return;
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
    use super::*;
    use crate::ConsumerGroupId;
    use crate::event::Event;
    use crate::io::Message;
    use crate::io::acker::NoopAcker;
    use crate::io::checkpoint::{
        CheckpointResumableSubscription, CheckpointResume, CheckpointResumePoint, StreamId,
    };
    use crate::io::readers::{PartitionedReader, PartitionedReaderConfig, PartitionedSubscription};
    use crate::payload::Payload;
    use crate::start_from::{StartFrom, StartableSubscription};
    use futures::Stream;
    use std::pin::Pin;
    use tokio::sync::Mutex as TokioMutex;

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

    #[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
    struct TestCursor(i64);

    impl CursorPartition for TestCursor {
        fn partition(&self) -> Option<LogicalPartition> {
            None
        }
    }
    impl CommitCursor for TestCursor {
        type Commit = TestCursor;
        fn commit_cursor(&self) -> Self::Commit {
            *self
        }
    }

    #[derive(Debug, Clone, Default)]
    struct TestSub;
    impl StartableSubscription<TestCursor> for TestSub {
        fn with_start(self, _: StartFrom<TestCursor>) -> Self {
            self
        }
    }
    impl CheckpointResumableSubscription<TestCursor> for TestSub {
        fn with_checkpoint_resume(self, resume: CheckpointResume<TestCursor>) -> Self {
            self.with_start(resume.start().clone())
        }
    }

    #[derive(Debug, Clone)]
    struct RecordingSub {
        start: StartFrom<TestCursor>,
        resume_points: Vec<CheckpointResumePoint<TestCursor>>,
    }

    impl Default for RecordingSub {
        fn default() -> Self {
            Self {
                start: StartFrom::Earliest,
                resume_points: Vec::new(),
            }
        }
    }

    impl CheckpointResumableSubscription<TestCursor> for RecordingSub {
        fn with_checkpoint_resume(mut self, resume: CheckpointResume<TestCursor>) -> Self {
            let (start, points) = resume.into_parts();
            self.start = start;
            self.resume_points = points;
            self
        }
    }

    struct RecordingSubReader {
        observed: std::sync::Arc<TokioMutex<Option<RecordingSub>>>,
    }

    impl Reader for RecordingSubReader {
        type Subscription = RecordingSub;
        type Acker = NoopAcker;
        type Cursor = TestCursor;
        type Stream = Pin<Box<dyn Stream<Item = Result<Message<NoopAcker, TestCursor>>> + Send>>;

        async fn read(&self, subscription: RecordingSub) -> Result<Self::Stream> {
            *self.observed.lock().await = Some(subscription);
            Ok(Box::pin(futures::stream::empty()))
        }
    }

    type PreloadedRows = Vec<(Option<LogicalPartition>, TestCursor)>;

    #[derive(Clone, Default)]
    struct PreloadedStore {
        rows: std::sync::Arc<TokioMutex<PreloadedRows>>,
    }

    impl CheckpointStore<TestCursor> for PreloadedStore {
        async fn load(&self, _: &CheckpointKey) -> Result<Option<TestCursor>> {
            Ok(None)
        }

        async fn load_scope(
            &self,
            _: &CheckpointScope,
        ) -> Result<Vec<(Option<LogicalPartition>, TestCursor)>> {
            Ok(self.rows.lock().await.clone())
        }

        async fn commit(&self, _: &CheckpointKey, _: TestCursor) -> Result<()> {
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

    #[derive(Clone, Default)]
    struct MemStore {
        rows: std::sync::Arc<TokioMutex<Vec<(CheckpointKey, TestCursor)>>>,
    }
    impl CheckpointStore<TestCursor> for MemStore {
        async fn load(&self, _: &CheckpointKey) -> Result<Option<TestCursor>> {
            Ok(None)
        }
        async fn load_scope(
            &self,
            _: &CheckpointScope,
        ) -> Result<Vec<(Option<LogicalPartition>, TestCursor)>> {
            Ok(vec![])
        }
        async fn commit(&self, key: &CheckpointKey, cursor: TestCursor) -> Result<()> {
            self.rows.lock().await.push((key.clone(), cursor));
            Ok(())
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

        let subscription = CheckpointSubscription::new(TestSub, scope)
            .with_resume_policy(CheckpointResumePolicy::Error);

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
        *store.rows.lock().await = vec![(None, TestCursor(10))];
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
        *store.rows.lock().await = vec![(None, TestCursor(10))];
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
                CheckpointSubscription::new(TestSub, scope)
                    .with_resume_policy(CheckpointResumePolicy::Error),
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
    async fn checkpoint_reader_passes_resume_points_to_inner_subscription() {
        let partition = LogicalPartition::new(2, std::num::NonZeroU16::new(4).unwrap()).unwrap();
        let store = PreloadedStore::default();
        *store.rows.lock().await = vec![(Some(partition), TestCursor(10))];
        let observed = std::sync::Arc::new(TokioMutex::new(None));
        let reader = RecordingSubReader {
            observed: std::sync::Arc::clone(&observed),
        };
        let cr = CheckpointReader::new(reader, store);
        let scope = CheckpointScope::new(
            ConsumerGroupId::new("g").unwrap(),
            StreamId::new("s").unwrap(),
        );

        let _stream = cr
            .read(CheckpointSubscription::new(RecordingSub::default(), scope))
            .await
            .unwrap();

        let observed = observed.lock().await.clone().unwrap();
        assert_eq!(observed.start, StartFrom::After(TestCursor(10)));
        assert_eq!(observed.resume_points.len(), 1);
        assert_eq!(observed.resume_points[0].partition(), Some(partition));
        assert_eq!(*observed.resume_points[0].cursor(), TestCursor(10));
    }

    #[derive(Debug, Clone)]
    struct PartitionedTestSub {
        start: StartFrom<TestCursor>,
    }

    impl Default for PartitionedTestSub {
        fn default() -> Self {
            Self {
                start: StartFrom::Earliest,
            }
        }
    }

    impl CheckpointResumableSubscription<TestCursor> for PartitionedTestSub {
        fn with_checkpoint_resume(mut self, resume: CheckpointResume<TestCursor>) -> Self {
            self.start = resume.start().clone();
            self
        }
    }

    struct OneEventReader;

    impl Reader for OneEventReader {
        type Subscription = PartitionedTestSub;
        type Acker = NoopAcker;
        type Cursor = TestCursor;
        type Stream = Pin<Box<dyn Stream<Item = Result<Message<NoopAcker, TestCursor>>> + Send>>;

        async fn read(&self, _: PartitionedTestSub) -> Result<Self::Stream> {
            Ok(Box::pin(futures::stream::iter(vec![Ok(Message::new(
                ev("k0"),
                NoopAcker,
                TestCursor(1),
            ))])))
        }
    }

    #[tokio::test]
    async fn checkpoint_reader_falls_back_when_partitioned_reader_rejects_old_partition_count() {
        use futures::StreamExt;
        let old_partition =
            LogicalPartition::new(0, std::num::NonZeroU16::new(8).unwrap()).unwrap();
        let store = PreloadedStore::default();
        *store.rows.lock().await = vec![(Some(old_partition), TestCursor(10))];
        let partitioned = PartitionedReader::new(
            OneEventReader,
            PartitionedReaderConfig {
                partition_count: std::num::NonZeroU16::new(4).unwrap(),
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
                PartitionedSubscription::new(PartitionedTestSub::default()),
                scope,
            ))
            .await
            .unwrap();

        let msg = stream.next().await.unwrap().unwrap();
        assert_eq!(msg.event().key().unwrap().as_str(), "k0");
    }

    #[tokio::test]
    async fn checkpoint_reader_errors_when_partitioned_reader_rejects_old_partition_count_and_policy_is_error()
     {
        let old_partition =
            LogicalPartition::new(0, std::num::NonZeroU16::new(8).unwrap()).unwrap();
        let store = PreloadedStore::default();
        *store.rows.lock().await = vec![(Some(old_partition), TestCursor(10))];
        let partitioned = PartitionedReader::new(
            OneEventReader,
            PartitionedReaderConfig {
                partition_count: std::num::NonZeroU16::new(4).unwrap(),
                ..PartitionedReaderConfig::default()
            },
        );
        let checkpointed = CheckpointReader::new(partitioned, store);
        let scope = CheckpointScope::new(
            ConsumerGroupId::new("g").unwrap(),
            StreamId::new("s").unwrap(),
        );

        let err = match checkpointed
            .read(
                CheckpointSubscription::new(
                    PartitionedSubscription::new(PartitionedTestSub::default()),
                    scope,
                )
                .with_resume_policy(CheckpointResumePolicy::Error),
            )
            .await
        {
            Ok(_) => panic!("expected invalid cursor error"),
            Err(e) => e,
        };

        assert!(matches!(err, Error::InvalidCursor(_)));
    }
}
