use std::num::NonZeroU32;

use chrono::{TimeZone, Utc};
use eventuary_core::io::handler::{MultiplexerKey, MultiplexerStore, SubscriberId};
use eventuary_core::io::reader::{BufferStore, DedupeStore, WatermarkStore};
use eventuary_core::partition::Partition;
use eventuary_core::{Event, Payload};
use eventuary_fs::buffer::{FsBufferStore, FsBufferStoreConfig};
use eventuary_fs::dedupe::{FsDedupeStore, FsDedupeStoreConfig};
use eventuary_fs::multiplexer::{FsMultiplexerStore, FsMultiplexerStoreConfig};
use eventuary_fs::reader::FsCursor;
use eventuary_fs::watermark::{FsWatermarkStore, FsWatermarkStoreConfig};

fn ev(key: &str) -> Event {
    Event::builder(
        "acme",
        "/orders",
        "order.created",
        key,
        Payload::from_string("p"),
    )
    .unwrap()
    .build()
    .unwrap()
}

fn cursor(offset: u64) -> FsCursor {
    FsCursor::new(
        Partition::new(0, NonZeroU32::new(1).unwrap()).unwrap(),
        offset,
    )
}

#[tokio::test]
async fn watermark_round_trips_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let ts = Utc.with_ymd_and_hms(2026, 8, 30, 12, 0, 0).unwrap();
    {
        let store = FsWatermarkStore::open(dir.path(), FsWatermarkStoreConfig::default()).unwrap();
        assert_eq!(store.load_watermark("orders").await.unwrap(), None);
        store.save_watermark("orders", ts).await.unwrap();
    }

    let store = FsWatermarkStore::open(dir.path(), FsWatermarkStoreConfig::default()).unwrap();

    assert_eq!(store.load_watermark("orders").await.unwrap(), Some(ts));
}

#[tokio::test]
async fn watermark_keys_are_isolated_and_encoded() {
    let dir = tempfile::tempdir().unwrap();
    let store = FsWatermarkStore::open(dir.path(), FsWatermarkStoreConfig::default()).unwrap();
    let ts = Utc.with_ymd_and_hms(2026, 8, 30, 12, 0, 0).unwrap();

    store.save_watermark("ns:/orders", ts).await.unwrap();

    assert_eq!(store.load_watermark("ns:/orders").await.unwrap(), Some(ts));
    assert_eq!(store.load_watermark("ns_orders").await.unwrap(), None);
}

#[tokio::test]
async fn watermark_save_overwrites() {
    let dir = tempfile::tempdir().unwrap();
    let store = FsWatermarkStore::open(dir.path(), FsWatermarkStoreConfig::default()).unwrap();
    let first = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let second = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();

    store.save_watermark("k", first).await.unwrap();
    store.save_watermark("k", second).await.unwrap();

    assert_eq!(store.load_watermark("k").await.unwrap(), Some(second));
}

#[tokio::test]
async fn dedupe_reports_unseen_then_seen() {
    let dir = tempfile::tempdir().unwrap();
    let store = FsDedupeStore::open(dir.path(), FsDedupeStoreConfig::default()).unwrap();
    let event = ev("a");

    assert!(!store.exists(&event).await.unwrap());
    store.mark_processed(&event).await.unwrap();
    assert!(store.exists(&event).await.unwrap());
}

#[tokio::test]
async fn dedupe_mark_if_new_is_atomic_and_reports_first_writer() {
    let dir = tempfile::tempdir().unwrap();
    let store = FsDedupeStore::open(dir.path(), FsDedupeStoreConfig::default()).unwrap();
    let event = ev("a");

    assert!(store.mark_if_new(&event).await.unwrap());
    assert!(!store.mark_if_new(&event).await.unwrap());
}

#[tokio::test]
async fn dedupe_only_one_concurrent_caller_wins() {
    let dir = tempfile::tempdir().unwrap();
    let store = FsDedupeStore::open(dir.path(), FsDedupeStoreConfig::default()).unwrap();
    let event = ev("a");

    let mut wins = 0;
    let mut handles = Vec::new();
    for _ in 0..16 {
        let store = store.clone();
        let event = event.clone();
        handles.push(tokio::spawn(async move {
            store.mark_if_new(&event).await.unwrap()
        }));
    }
    for handle in handles {
        if handle.await.unwrap() {
            wins += 1;
        }
    }

    assert_eq!(wins, 1);
}

#[tokio::test]
async fn dedupe_distinguishes_distinct_events() {
    let dir = tempfile::tempdir().unwrap();
    let store = FsDedupeStore::open(dir.path(), FsDedupeStoreConfig::default()).unwrap();

    store.mark_processed(&ev("a")).await.unwrap();

    assert!(!store.exists(&ev("b")).await.unwrap());
}

#[tokio::test]
async fn dedupe_markers_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let event = ev("a");
    {
        let store = FsDedupeStore::open(dir.path(), FsDedupeStoreConfig::default()).unwrap();
        store.mark_processed(&event).await.unwrap();
    }

    let store = FsDedupeStore::open(dir.path(), FsDedupeStoreConfig::default()).unwrap();

    assert!(store.exists(&event).await.unwrap());
}

#[tokio::test]
async fn multiplexer_tracks_completion_per_subscriber() {
    let dir = tempfile::tempdir().unwrap();
    let store = FsMultiplexerStore::open(dir.path(), FsMultiplexerStoreConfig::default()).unwrap();
    let event = ev("a");
    let a = MultiplexerKey::new(event.id(), SubscriberId::new("projector").unwrap());
    let b = MultiplexerKey::new(event.id(), SubscriberId::new("mailer").unwrap());

    store.mark_completed(&a).await.unwrap();

    assert!(store.is_completed(&a).await.unwrap());
    assert!(!store.is_completed(&b).await.unwrap());
}

#[tokio::test]
async fn multiplexer_mark_completed_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let store = FsMultiplexerStore::open(dir.path(), FsMultiplexerStoreConfig::default()).unwrap();
    let key = MultiplexerKey::new(ev("a").id(), SubscriberId::new("projector").unwrap());

    store.mark_completed(&key).await.unwrap();
    store.mark_completed(&key).await.unwrap();

    assert!(store.is_completed(&key).await.unwrap());
}

#[tokio::test]
async fn multiplexer_completion_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let key = MultiplexerKey::new(ev("a").id(), SubscriberId::new("projector").unwrap());
    {
        let store =
            FsMultiplexerStore::open(dir.path(), FsMultiplexerStoreConfig::default()).unwrap();
        store.mark_completed(&key).await.unwrap();
    }

    let store = FsMultiplexerStore::open(dir.path(), FsMultiplexerStoreConfig::default()).unwrap();

    assert!(store.is_completed(&key).await.unwrap());
}

#[tokio::test]
async fn buffer_push_returns_increasing_ids_and_pending_is_ordered() {
    let dir = tempfile::tempdir().unwrap();
    let store: FsBufferStore<FsCursor> =
        FsBufferStore::open(dir.path(), FsBufferStoreConfig::default()).unwrap();

    let first = store.push(&ev("a"), &cursor(0)).await.unwrap();
    let second = store.push(&ev("b"), &cursor(1)).await.unwrap();
    let pending = store.pending().await.unwrap();

    assert!(first.0 < second.0);
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].id, first);
    assert_eq!(pending[1].id, second);
    assert_eq!(pending[0].event.key().as_str(), "a");
    assert_eq!(pending[0].cursor.offset, 0);
}

#[tokio::test]
async fn buffer_pending_is_repeatable_until_acked() {
    let dir = tempfile::tempdir().unwrap();
    let store: FsBufferStore<FsCursor> =
        FsBufferStore::open(dir.path(), FsBufferStoreConfig::default()).unwrap();
    store.push(&ev("a"), &cursor(0)).await.unwrap();

    assert_eq!(store.pending().await.unwrap().len(), 1);
    assert_eq!(store.pending().await.unwrap().len(), 1);
}

#[tokio::test]
async fn buffer_ack_removes_and_nack_retains() {
    let dir = tempfile::tempdir().unwrap();
    let store: FsBufferStore<FsCursor> =
        FsBufferStore::open(dir.path(), FsBufferStoreConfig::default()).unwrap();
    let a = store.push(&ev("a"), &cursor(0)).await.unwrap();
    let b = store.push(&ev("b"), &cursor(1)).await.unwrap();

    store.ack(&a).await.unwrap();
    store.nack(&b).await.unwrap();
    let pending = store.pending().await.unwrap();

    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, b);
}

#[tokio::test]
async fn buffer_ack_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let store: FsBufferStore<FsCursor> =
        FsBufferStore::open(dir.path(), FsBufferStoreConfig::default()).unwrap();
    let id = store.push(&ev("a"), &cursor(0)).await.unwrap();

    store.ack(&id).await.unwrap();
    store.ack(&id).await.unwrap();

    assert!(store.pending().await.unwrap().is_empty());
}

#[tokio::test]
async fn buffer_entries_survive_reopen_and_ids_do_not_collide() {
    let dir = tempfile::tempdir().unwrap();
    let first_id = {
        let store: FsBufferStore<FsCursor> =
            FsBufferStore::open(dir.path(), FsBufferStoreConfig::default()).unwrap();
        store.push(&ev("a"), &cursor(0)).await.unwrap()
    };

    let store: FsBufferStore<FsCursor> =
        FsBufferStore::open(dir.path(), FsBufferStoreConfig::default()).unwrap();
    let second_id = store.push(&ev("b"), &cursor(1)).await.unwrap();
    let pending = store.pending().await.unwrap();

    assert!(second_id.0 > first_id.0);
    assert_eq!(pending.len(), 2);
}
