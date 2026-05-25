use sqlx::PgPool;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

use chrono::Utc;

use eventuary_core::io::StreamId;
use eventuary_core::io::cursor::CursorOrder;
use eventuary_core::io::handler::{MultiplexerKey, MultiplexerStore, SubscriberId};
use eventuary_core::io::reader::{BufferStore, CheckpointStore, DedupeStore, WatermarkStore};
use eventuary_core::io::{
    ConsumerGroupId, Cursor,
    reader::{CheckpointKey, CheckpointScope},
};
use eventuary_core::{Event, EventId, Payload};
use eventuary_postgres::database::PgDatabase;
use eventuary_postgres::{
    PgBufferStore, PgBufferStoreConfig, PgCheckpointStore, PgCheckpointStoreConfig, PgDedupeStore,
    PgDedupeStoreConfig, PgMultiplexerStore, PgMultiplexerStoreConfig, PgWatermarkStore,
    PgWatermarkStoreConfig,
};

async fn start_postgres() -> (ContainerAsync<GenericImage>, PgPool) {
    let container = GenericImage::new("postgres", "18-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "eventuary")
        .with_env_var("POSTGRES_PASSWORD", "eventuary")
        .with_env_var("POSTGRES_DB", "eventuary")
        .start()
        .await
        .expect("postgres start");
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://eventuary:eventuary@127.0.0.1:{port}/eventuary");
    let db = PgDatabase::connect(&url).await.unwrap();
    let pool = db.pool();
    (container, pool)
}

fn ev(topic: &str) -> Event {
    Event::builder("acme", "/x", topic, Payload::from_string("p"))
        .unwrap()
        .key("k")
        .unwrap()
        .build()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_multiplexer_store_records_and_skips_completed() {
    let (_c, pool) = start_postgres().await;
    let store = PgMultiplexerStore::new(pool, PgMultiplexerStoreConfig::default());
    let key = MultiplexerKey::new(EventId::new(), SubscriberId::new("audit").unwrap());

    assert!(!store.is_completed(&key).await.unwrap());
    store.mark_completed(&key).await.unwrap();
    assert!(store.is_completed(&key).await.unwrap());

    // Idempotent re-mark
    store.mark_completed(&key).await.unwrap();
    assert!(store.is_completed(&key).await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_multiplexer_store_scopes_by_subscriber() {
    let (_c, pool) = start_postgres().await;
    let store = PgMultiplexerStore::new(pool, PgMultiplexerStoreConfig::default());
    let event_id = EventId::new();
    let k1 = MultiplexerKey::new(event_id, SubscriberId::new("a").unwrap());
    let k2 = MultiplexerKey::new(event_id, SubscriberId::new("b").unwrap());

    store.mark_completed(&k1).await.unwrap();
    assert!(store.is_completed(&k1).await.unwrap());
    assert!(!store.is_completed(&k2).await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_dedupe_store_marks_and_finds_events() {
    let (_c, pool) = start_postgres().await;
    let store = PgDedupeStore::new(pool, PgDedupeStoreConfig::default());
    let event = ev("t1");

    assert!(!store.exists(&event).await.unwrap());
    store.mark_processed(&event).await.unwrap();
    assert!(store.exists(&event).await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_dedupe_store_mark_if_new_returns_false_on_duplicate() {
    let (_c, pool) = start_postgres().await;
    let store = PgDedupeStore::new(pool, PgDedupeStoreConfig::default());
    let event = ev("t1");

    assert!(store.mark_if_new(&event).await.unwrap());
    assert!(!store.mark_if_new(&event).await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_buffer_store_round_trips_entry() {
    let (_c, pool) = start_postgres().await;
    let store: PgBufferStore<i64> = PgBufferStore::new(pool, PgBufferStoreConfig::default());
    let event = ev("t1");
    let cursor: i64 = 42;

    let id = store.push(&event, &cursor).await.unwrap();
    let pending = store.pending().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, id);
    assert_eq!(pending[0].event.id(), event.id());
    assert_eq!(pending[0].cursor, cursor);

    store.ack(&id).await.unwrap();
    let pending_after = store.pending().await.unwrap();
    assert!(pending_after.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_buffer_store_nack_keeps_entry() {
    let (_c, pool) = start_postgres().await;
    let store: PgBufferStore<i64> = PgBufferStore::new(pool, PgBufferStoreConfig::default());
    let id = store.push(&ev("t1"), &1_i64).await.unwrap();
    store.nack(&id).await.unwrap();
    let pending = store.pending().await.unwrap();
    assert_eq!(pending.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_buffer_store_pending_orders_by_id() {
    let (_c, pool) = start_postgres().await;
    let store: PgBufferStore<i64> = PgBufferStore::new(pool, PgBufferStoreConfig::default());
    let id1 = store.push(&ev("t1"), &1_i64).await.unwrap();
    let id2 = store.push(&ev("t2"), &2_i64).await.unwrap();
    let id3 = store.push(&ev("t3"), &3_i64).await.unwrap();

    let pending = store.pending().await.unwrap();
    assert_eq!(pending.len(), 3);
    assert_eq!(pending[0].id, id1);
    assert_eq!(pending[1].id, id2);
    assert_eq!(pending[2].id, id3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_watermark_store_save_and_load() {
    let (_c, pool) = start_postgres().await;
    let store = PgWatermarkStore::new(pool, PgWatermarkStoreConfig::default());
    assert!(store.load_watermark("k").await.unwrap().is_none());

    let now = Utc::now();
    store.save_watermark("k", now).await.unwrap();
    let loaded = store.load_watermark("k").await.unwrap().unwrap();
    assert_eq!(loaded.timestamp_millis(), now.timestamp_millis());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_watermark_store_save_overwrites() {
    let (_c, pool) = start_postgres().await;
    let store = PgWatermarkStore::new(pool, PgWatermarkStoreConfig::default());
    let t1 = Utc::now();
    let t2 = t1 + chrono::Duration::seconds(60);
    store.save_watermark("k", t1).await.unwrap();
    store.save_watermark("k", t2).await.unwrap();
    let loaded = store.load_watermark("k").await.unwrap().unwrap();
    assert_eq!(loaded.timestamp_millis(), t2.timestamp_millis());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_watermark_store_scopes_by_key() {
    let (_c, pool) = start_postgres().await;
    let store = PgWatermarkStore::new(pool, PgWatermarkStoreConfig::default());
    let now = Utc::now();
    store.save_watermark("a", now).await.unwrap();
    assert!(store.load_watermark("a").await.unwrap().is_some());
    assert!(store.load_watermark("b").await.unwrap().is_none());
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SeqCursor(i64);

impl Cursor for SeqCursor {
    fn order_key(&self) -> CursorOrder {
        CursorOrder::from_i64(self.0)
    }
}

fn checkpoint_key() -> CheckpointKey {
    CheckpointKey::new(
        CheckpointScope::new(
            ConsumerGroupId::new("test-group").unwrap(),
            StreamId::new("test-stream").unwrap(),
        ),
        eventuary_core::io::CursorId::global(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_checkpoint_store_regression_guard_rejects_older_cursor() {
    let (_c, pool) = start_postgres().await;
    let store: PgCheckpointStore<SeqCursor> =
        PgCheckpointStore::new(pool, PgCheckpointStoreConfig::default());
    let key = checkpoint_key();

    store.commit(&key, SeqCursor(100)).await.unwrap();
    store.commit(&key, SeqCursor(50)).await.unwrap();

    let loaded = store.load(&key).await.unwrap().unwrap();
    assert_eq!(loaded.0, 100);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_checkpoint_store_forward_commit_advances_cursor() {
    let (_c, pool) = start_postgres().await;
    let store: PgCheckpointStore<SeqCursor> =
        PgCheckpointStore::new(pool, PgCheckpointStoreConfig::default());
    let key = checkpoint_key();

    store.commit(&key, SeqCursor(100)).await.unwrap();
    store.commit(&key, SeqCursor(200)).await.unwrap();

    let loaded = store.load(&key).await.unwrap().unwrap();
    assert_eq!(loaded.0, 200);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_checkpoint_store_idempotent_recommit_is_safe() {
    let (_c, pool) = start_postgres().await;
    let store: PgCheckpointStore<SeqCursor> =
        PgCheckpointStore::new(pool, PgCheckpointStoreConfig::default());
    let key = checkpoint_key();

    store.commit(&key, SeqCursor(100)).await.unwrap();
    store.commit(&key, SeqCursor(100)).await.unwrap();

    let loaded = store.load(&key).await.unwrap().unwrap();
    assert_eq!(loaded.0, 100);

    store.commit(&key, SeqCursor(150)).await.unwrap();
    let loaded = store.load(&key).await.unwrap().unwrap();
    assert_eq!(loaded.0, 150);
}
