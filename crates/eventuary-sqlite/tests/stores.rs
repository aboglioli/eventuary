use chrono::Utc;

use eventuary_core::io::handler::{MultiplexerKey, MultiplexerStore, SubscriberId};
use eventuary_core::io::reader::{BufferStore, DedupeStore, WatermarkStore};
use eventuary_core::{Event, EventId, Payload};
use eventuary_sqlite::database::{SqliteConn, SqliteDatabase};
use eventuary_sqlite::{
    SqliteBufferStore, SqliteBufferStoreConfig, SqliteDedupeStore, SqliteDedupeStoreConfig,
    SqliteMultiplexerStore, SqliteMultiplexerStoreConfig, SqliteWatermarkStore,
    SqliteWatermarkStoreConfig,
};

fn prepare_test_schema(conn: &SqliteConn) {
    SqliteMultiplexerStore::prepare_schema(conn, &SqliteMultiplexerStoreConfig::default()).unwrap();
    SqliteDedupeStore::prepare_schema(conn, &SqliteDedupeStoreConfig::default()).unwrap();
    SqliteBufferStore::<i64>::prepare_schema(conn, &SqliteBufferStoreConfig::default()).unwrap();
    SqliteWatermarkStore::prepare_schema(conn, &SqliteWatermarkStoreConfig::default()).unwrap();
}

fn ev(topic: &str) -> Event {
    Event::builder("acme", "/x", topic, "k", Payload::from_string("p"))
        .unwrap()
        .build()
        .unwrap()
}

#[tokio::test]
async fn sqlite_multiplexer_store_records_and_skips_completed() {
    let db = SqliteDatabase::open_in_memory().unwrap();
    prepare_test_schema(&db.conn());
    let store = SqliteMultiplexerStore::new(db.conn(), SqliteMultiplexerStoreConfig::default());
    let key = MultiplexerKey::new(EventId::new(), SubscriberId::new("audit").unwrap());

    assert!(!store.is_completed(&key).await.unwrap());
    store.mark_completed(&key).await.unwrap();
    assert!(store.is_completed(&key).await.unwrap());

    store.mark_completed(&key).await.unwrap();
    assert!(store.is_completed(&key).await.unwrap());
}

#[tokio::test]
async fn sqlite_multiplexer_store_scopes_by_subscriber() {
    let db = SqliteDatabase::open_in_memory().unwrap();
    prepare_test_schema(&db.conn());
    let store = SqliteMultiplexerStore::new(db.conn(), SqliteMultiplexerStoreConfig::default());
    let event_id = EventId::new();
    let k1 = MultiplexerKey::new(event_id, SubscriberId::new("a").unwrap());
    let k2 = MultiplexerKey::new(event_id, SubscriberId::new("b").unwrap());

    store.mark_completed(&k1).await.unwrap();
    assert!(store.is_completed(&k1).await.unwrap());
    assert!(!store.is_completed(&k2).await.unwrap());
}

#[tokio::test]
async fn sqlite_dedupe_store_marks_and_finds_events() {
    let db = SqliteDatabase::open_in_memory().unwrap();
    prepare_test_schema(&db.conn());
    let store = SqliteDedupeStore::new(db.conn(), SqliteDedupeStoreConfig::default());
    let event = ev("t1");

    assert!(!store.exists(&event).await.unwrap());
    store.mark_processed(&event).await.unwrap();
    assert!(store.exists(&event).await.unwrap());
}

#[tokio::test]
async fn sqlite_dedupe_store_mark_if_new_returns_false_on_duplicate() {
    let db = SqliteDatabase::open_in_memory().unwrap();
    prepare_test_schema(&db.conn());
    let store = SqliteDedupeStore::new(db.conn(), SqliteDedupeStoreConfig::default());
    let event = ev("t1");

    assert!(store.mark_if_new(&event).await.unwrap());
    assert!(!store.mark_if_new(&event).await.unwrap());
}

#[tokio::test]
async fn sqlite_buffer_store_round_trips_entry() {
    let db = SqliteDatabase::open_in_memory().unwrap();
    prepare_test_schema(&db.conn());
    let store: SqliteBufferStore<i64> =
        SqliteBufferStore::new(db.conn(), SqliteBufferStoreConfig::default());
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

#[tokio::test]
async fn sqlite_buffer_store_nack_keeps_entry() {
    let db = SqliteDatabase::open_in_memory().unwrap();
    prepare_test_schema(&db.conn());
    let store: SqliteBufferStore<i64> =
        SqliteBufferStore::new(db.conn(), SqliteBufferStoreConfig::default());
    let id = store.push(&ev("t1"), &1_i64).await.unwrap();
    store.nack(&id).await.unwrap();
    let pending = store.pending().await.unwrap();
    assert_eq!(pending.len(), 1);
}

#[tokio::test]
async fn sqlite_buffer_store_pending_orders_by_id() {
    let db = SqliteDatabase::open_in_memory().unwrap();
    prepare_test_schema(&db.conn());
    let store: SqliteBufferStore<i64> =
        SqliteBufferStore::new(db.conn(), SqliteBufferStoreConfig::default());
    let id1 = store.push(&ev("t1"), &1_i64).await.unwrap();
    let id2 = store.push(&ev("t2"), &2_i64).await.unwrap();
    let id3 = store.push(&ev("t3"), &3_i64).await.unwrap();

    let pending = store.pending().await.unwrap();
    assert_eq!(pending.len(), 3);
    assert_eq!(pending[0].id, id1);
    assert_eq!(pending[1].id, id2);
    assert_eq!(pending[2].id, id3);
}

#[tokio::test]
async fn sqlite_watermark_store_save_and_load() {
    let db = SqliteDatabase::open_in_memory().unwrap();
    prepare_test_schema(&db.conn());
    let store = SqliteWatermarkStore::new(db.conn(), SqliteWatermarkStoreConfig::default());
    assert!(store.load_watermark("k").await.unwrap().is_none());

    let now = Utc::now();
    store.save_watermark("k", now).await.unwrap();
    let loaded = store.load_watermark("k").await.unwrap().unwrap();
    assert_eq!(loaded.timestamp_millis(), now.timestamp_millis());
}

#[tokio::test]
async fn sqlite_watermark_store_save_overwrites() {
    let db = SqliteDatabase::open_in_memory().unwrap();
    prepare_test_schema(&db.conn());
    let store = SqliteWatermarkStore::new(db.conn(), SqliteWatermarkStoreConfig::default());
    let t1 = Utc::now();
    let t2 = t1 + chrono::Duration::seconds(60);
    store.save_watermark("k", t1).await.unwrap();
    store.save_watermark("k", t2).await.unwrap();
    let loaded = store.load_watermark("k").await.unwrap().unwrap();
    assert_eq!(loaded.timestamp_millis(), t2.timestamp_millis());
}

#[tokio::test]
async fn sqlite_watermark_store_scopes_by_key() {
    let db = SqliteDatabase::open_in_memory().unwrap();
    prepare_test_schema(&db.conn());
    let store = SqliteWatermarkStore::new(db.conn(), SqliteWatermarkStoreConfig::default());
    let now = Utc::now();
    store.save_watermark("a", now).await.unwrap();
    assert!(store.load_watermark("a").await.unwrap().is_some());
    assert!(store.load_watermark("b").await.unwrap().is_none());
}
