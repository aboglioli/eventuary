//! In-memory [`WatermarkStore`] implementation.
//!
//! Stores per-key high-water timestamps in a `HashMap`. Suitable for
//! development, tests, and single-process use.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use chrono::{DateTime, Utc};

use eventuary_core::Result;
use eventuary_core::io::reader::WatermarkStore;

#[derive(Debug, Clone, Default)]
pub struct MemoryWatermarkStore {
    state: Arc<Mutex<HashMap<String, DateTime<Utc>>>>,
}

impl MemoryWatermarkStore {
    pub fn new() -> Self {
        Self::default()
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

impl WatermarkStore for MemoryWatermarkStore {
    async fn load_watermark(&self, key: &str) -> Result<Option<DateTime<Utc>>> {
        Ok(self.state.lock().unwrap().get(key).copied())
    }

    async fn save_watermark(&self, key: &str, ts: DateTime<Utc>) -> Result<()> {
        self.state.lock().unwrap().insert(key.to_owned(), ts);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn save_and_load() {
        let store = MemoryWatermarkStore::new();
        assert!(store.load_watermark("k").await.unwrap().is_none());
        let now = Utc::now();
        store.save_watermark("k", now).await.unwrap();
        assert_eq!(store.load_watermark("k").await.unwrap(), Some(now));
    }

    #[tokio::test]
    async fn save_overwrites() {
        let store = MemoryWatermarkStore::new();
        let t1 = Utc::now();
        let t2 = t1 + chrono::Duration::seconds(60);
        store.save_watermark("k", t1).await.unwrap();
        store.save_watermark("k", t2).await.unwrap();
        assert_eq!(store.load_watermark("k").await.unwrap(), Some(t2));
    }

    #[tokio::test]
    async fn scopes_by_key() {
        let store = MemoryWatermarkStore::new();
        let now = Utc::now();
        store.save_watermark("a", now).await.unwrap();
        assert_eq!(store.load_watermark("a").await.unwrap(), Some(now));
        assert!(store.load_watermark("b").await.unwrap().is_none());
    }
}
