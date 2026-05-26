//! In-memory [`CheckpointStore`] implementation.
//!
//! Holds checkpoint cursors in a `HashMap` keyed by
//! `(scope, cursor_id)`. Suitable for development, tests, and
//! single-process use. For durable consumer offsets across restarts,
//! use a backend-backed store.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use eventuary_core::Result;
use eventuary_core::io::CursorId;
use eventuary_core::io::reader::{CheckpointKey, CheckpointScope, CheckpointStore};

pub struct MemoryCheckpointStore<C> {
    state: Arc<Mutex<HashMap<CheckpointKey, C>>>,
}

impl<C> Clone for MemoryCheckpointStore<C> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl<C> Default for MemoryCheckpointStore<C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C> MemoryCheckpointStore<C> {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn len(&self) -> usize {
        self.state.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.state.lock().unwrap().is_empty()
    }

    pub fn clear(&self) {
        self.state.lock().unwrap().clear();
    }
}

impl<C> CheckpointStore<C> for MemoryCheckpointStore<C>
where
    C: Clone + Send + Sync + 'static,
{
    async fn load(&self, key: &CheckpointKey) -> Result<Option<C>> {
        Ok(self.state.lock().unwrap().get(key).cloned())
    }

    async fn load_scope(&self, scope: &CheckpointScope) -> Result<Vec<(CursorId, C)>> {
        let state = self.state.lock().unwrap();
        let mut out: Vec<(CursorId, C)> = state
            .iter()
            .filter(|(k, _)| &k.scope == scope)
            .map(|(k, v)| (k.cursor_id.clone(), v.clone()))
            .collect();
        out.sort_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
        Ok(out)
    }

    async fn commit(&self, key: &CheckpointKey, cursor: C) -> Result<()> {
        self.state.lock().unwrap().insert(key.clone(), cursor);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use eventuary_core::io::ConsumerGroupId;
    use eventuary_core::io::StreamId;

    fn scope() -> CheckpointScope {
        CheckpointScope::new(
            ConsumerGroupId::new("group").unwrap(),
            StreamId::new("stream").unwrap(),
        )
    }

    fn key(cursor_id: CursorId) -> CheckpointKey {
        CheckpointKey::new(scope(), cursor_id)
    }

    #[tokio::test]
    async fn commit_and_load() {
        let store: MemoryCheckpointStore<i64> = MemoryCheckpointStore::new();
        let k = key(CursorId::global());

        assert!(store.load(&k).await.unwrap().is_none());
        store.commit(&k, 42).await.unwrap();
        assert_eq!(store.load(&k).await.unwrap(), Some(42));
    }

    #[tokio::test]
    async fn commit_overwrites() {
        let store: MemoryCheckpointStore<i64> = MemoryCheckpointStore::new();
        let k = key(CursorId::global());
        store.commit(&k, 1).await.unwrap();
        store.commit(&k, 2).await.unwrap();
        assert_eq!(store.load(&k).await.unwrap(), Some(2));
    }

    #[tokio::test]
    async fn load_scope_returns_all_cursors_in_scope() {
        let store: MemoryCheckpointStore<i64> = MemoryCheckpointStore::new();
        let count = std::num::NonZeroU16::new(4).unwrap();
        let k1 = key(CursorId::partition(
            eventuary_core::partition::Partition::new(0, count).unwrap(),
        ));
        let k2 = key(CursorId::partition(
            eventuary_core::partition::Partition::new(1, count).unwrap(),
        ));
        store.commit(&k1, 10).await.unwrap();
        store.commit(&k2, 20).await.unwrap();

        let entries = store.load_scope(&scope()).await.unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[tokio::test]
    async fn load_scope_filters_by_scope() {
        let store: MemoryCheckpointStore<i64> = MemoryCheckpointStore::new();
        let other_scope = CheckpointScope::new(
            ConsumerGroupId::new("other").unwrap(),
            StreamId::new("stream").unwrap(),
        );

        store.commit(&key(CursorId::global()), 1).await.unwrap();
        store
            .commit(
                &CheckpointKey::new(other_scope.clone(), CursorId::global()),
                99,
            )
            .await
            .unwrap();

        assert_eq!(store.load_scope(&scope()).await.unwrap().len(), 1);
        assert_eq!(store.load_scope(&other_scope).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn stores_encoded_cursor() {
        use eventuary_core::io::cursor::{CursorKind, CursorOrder, EncodedCursor};

        let store: MemoryCheckpointStore<EncodedCursor> = MemoryCheckpointStore::new();
        let k = key(CursorId::global());
        let cursor = EncodedCursor::from_json(
            CursorId::global(),
            CursorKind::new("eventuary.test.cursor.v1").unwrap(),
            CursorOrder::from_i64(42),
            &serde_json::json!({ "sequence": 42 }),
        )
        .unwrap();

        store.commit(&k, cursor.clone()).await.unwrap();
        assert_eq!(store.load(&k).await.unwrap(), Some(cursor));
    }
}
