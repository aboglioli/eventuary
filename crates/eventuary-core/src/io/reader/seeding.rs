use std::future::Future;

use crate::error::Result;
use crate::io::position::StartableSubscription;
use crate::io::reader::checkpoint::{
    CheckpointAcker, CheckpointKey, CheckpointReader, CheckpointReaderConfig, CheckpointStore,
    CheckpointStream, CheckpointSubscription,
};
use crate::io::{Acker, Cursor, Reader};

pub trait StartResolver<S, C>: Clone + Send + Sync + 'static {
    fn resolve(&self, subscription: &S) -> impl Future<Output = Result<C>> + Send;
}

pub struct SeedingCheckpointReader<R, X, S> {
    resolver: X,
    store: S,
    checkpoint: CheckpointReader<R, S>,
}

impl<R, X, S: Clone> SeedingCheckpointReader<R, X, S> {
    pub fn new(inner: R, resolver: X, store: S) -> Self {
        Self::with_config(inner, resolver, store, CheckpointReaderConfig::default())
    }

    pub fn with_config(inner: R, resolver: X, store: S, config: CheckpointReaderConfig) -> Self {
        let checkpoint = CheckpointReader::with_config(inner, store.clone(), config);
        Self {
            resolver,
            store,
            checkpoint,
        }
    }
}

impl<R, X, S, P> Reader<P> for SeedingCheckpointReader<R, X, S>
where
    R: Reader<P> + Send + Sync + 'static,
    R::Cursor: Cursor + Clone + Ord + Send + Sync + 'static,
    R::Subscription: StartableSubscription<R::Cursor>,
    R::Acker: Acker + Send + Sync + 'static,
    R::Stream: Send + 'static,
    X: StartResolver<R::Subscription, R::Cursor>,
    S: CheckpointStore<R::Cursor>,
    P: Send + Sync + 'static,
{
    type Subscription = CheckpointSubscription<R::Subscription>;
    type Acker = CheckpointAcker<R::Acker, R::Cursor, S>;
    type Cursor = R::Cursor;
    type Stream = CheckpointStream<R::Acker, R::Cursor, S, P>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let scope = &subscription.scope;
        if self.store.load_scope(scope).await?.is_empty() {
            let cursor = self.resolver.resolve(&subscription.inner).await?;
            let key = CheckpointKey::new(scope.clone(), cursor.id());
            self.store.commit(&key, cursor).await?;
        }
        self.checkpoint.read(subscription).await
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};

    use futures::Stream;

    use super::*;
    use crate::io::acker::NoopAcker;
    use crate::io::position::StartFrom;
    use crate::io::reader::checkpoint::CheckpointScope;
    use crate::io::{ConsumerGroupId, CursorId, Message, StreamId};

    #[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
    struct LogCursor(u64);

    impl Cursor for LogCursor {}

    #[derive(Debug, Clone)]
    struct LogSubscription {
        start: StartFrom<LogCursor>,
    }

    impl StartableSubscription<LogCursor> for LogSubscription {
        fn with_start(mut self, start: StartFrom<LogCursor>) -> Self {
            self.start = start;
            self
        }
    }

    struct LogReader {
        observed: Arc<Mutex<Vec<StartFrom<LogCursor>>>>,
    }

    impl Reader for LogReader {
        type Subscription = LogSubscription;
        type Acker = NoopAcker;
        type Cursor = LogCursor;
        type Stream = Pin<Box<dyn Stream<Item = Result<Message<NoopAcker, LogCursor>>> + Send>>;

        async fn read(&self, subscription: LogSubscription) -> Result<Self::Stream> {
            self.observed.lock().unwrap().push(subscription.start);
            Ok(Box::pin(futures::stream::empty()))
        }
    }

    #[derive(Clone)]
    struct FixedResolver(u64);

    impl StartResolver<LogSubscription, LogCursor> for FixedResolver {
        async fn resolve(&self, _: &LogSubscription) -> Result<LogCursor> {
            Ok(LogCursor(self.0))
        }
    }

    #[derive(Clone, Default)]
    struct MemoryStore {
        committed: Arc<Mutex<Vec<(CursorId, LogCursor)>>>,
    }

    impl CheckpointStore<LogCursor> for MemoryStore {
        async fn load(&self, key: &CheckpointKey) -> Result<Option<LogCursor>> {
            Ok(self
                .committed
                .lock()
                .unwrap()
                .iter()
                .rev()
                .find(|(id, _)| *id == key.cursor_id)
                .map(|(_, cursor)| *cursor))
        }

        async fn load_scope(&self, _: &CheckpointScope) -> Result<Vec<(CursorId, LogCursor)>> {
            Ok(self.committed.lock().unwrap().clone())
        }

        async fn commit(&self, key: &CheckpointKey, cursor: LogCursor) -> Result<()> {
            self.committed
                .lock()
                .unwrap()
                .push((key.cursor_id.clone(), cursor));
            Ok(())
        }
    }

    fn scope() -> CheckpointScope {
        CheckpointScope::new(
            ConsumerGroupId::new("group").unwrap(),
            StreamId::new("events").unwrap(),
        )
    }

    fn subscription() -> CheckpointSubscription<LogSubscription> {
        CheckpointSubscription::new(
            LogSubscription {
                start: StartFrom::Latest,
            },
            scope(),
        )
    }

    #[tokio::test]
    async fn a_missing_checkpoint_is_seeded_with_the_resolved_start_before_reading() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let store = MemoryStore::default();
        let reader = SeedingCheckpointReader::new(
            LogReader {
                observed: Arc::clone(&observed),
            },
            FixedResolver(7),
            store.clone(),
        );

        let _stream = reader.read(subscription()).await.unwrap();

        assert_eq!(
            *store.committed.lock().unwrap(),
            vec![(CursorId::global(), LogCursor(7))]
        );
        assert_eq!(
            *observed.lock().unwrap(),
            vec![StartFrom::After(LogCursor(7))]
        );
    }

    #[tokio::test]
    async fn an_existing_checkpoint_is_kept_and_nothing_is_seeded() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let store = MemoryStore::default();
        store
            .commit(
                &CheckpointKey::new(scope(), CursorId::global()),
                LogCursor(3),
            )
            .await
            .unwrap();
        let reader = SeedingCheckpointReader::new(
            LogReader {
                observed: Arc::clone(&observed),
            },
            FixedResolver(7),
            store.clone(),
        );

        let _stream = reader.read(subscription()).await.unwrap();

        assert_eq!(store.committed.lock().unwrap().len(), 1);
        assert_eq!(
            *observed.lock().unwrap(),
            vec![StartFrom::After(LogCursor(3))]
        );
    }
}
