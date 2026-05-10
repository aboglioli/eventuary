use std::time::Duration;

use chrono::Utc;
use futures::StreamExt;
use tempfile::NamedTempFile;
use tokio::time::timeout;

use eventuary::io::Writer;
use eventuary::{ConsumerGroupId, Event, Namespace, OrganizationId, Payload, StartFrom, Topic};

use eventuary_sqlite::{SqliteDatabase, SqliteEventWriter, SqliteReader, SqliteReaderConfig};

fn fresh_db() -> (NamedTempFile, SqliteDatabase) {
    let file = NamedTempFile::new().unwrap();
    let db = SqliteDatabase::open(file.path()).unwrap();
    (file, db)
}

fn ev(org: &str, ns: &str, topic: &str, key: &str) -> Event {
    Event::create(org, ns, topic, key, Payload::from_string("payload")).unwrap()
}

fn config(org: &str) -> SqliteReaderConfig {
    SqliteReaderConfig {
        organization: OrganizationId::new(org).unwrap(),
        namespace: None,
        topics: Vec::new(),
        consumer_group_id: None,
        stream: "default".to_owned(),
        start_from: StartFrom::Earliest,
        poll_interval: Duration::from_millis(20),
        batch_size: 10,
    }
}

#[tokio::test]
async fn write_persists_event() {
    let (_file, db) = fresh_db();
    let writer = SqliteEventWriter::new(db.conn());
    let event = ev("acme", "/x", "thing.happened", "k");
    writer.write(&event).await.unwrap();

    let conn = db.conn();
    let guard = conn.lock().unwrap();
    let count: i64 = guard
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 1);
    let topic: String = guard
        .query_row("SELECT topic FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(topic, "thing.happened");
}

#[tokio::test]
async fn reader_streams_existing_events_from_earliest() {
    let (_file, db) = fresh_db();
    let writer = SqliteEventWriter::new(db.conn());
    for i in 0..3 {
        writer
            .write(&ev("acme", "/x", "thing.happened", &format!("k{i}")))
            .await
            .unwrap();
    }
    let reader = SqliteReader::new(db.conn(), config("acme"));
    let mut stream = reader.read().await.unwrap();
    let mut keys = Vec::new();
    for _ in 0..3 {
        let msg = timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        keys.push(msg.event().key().as_str().to_owned());
        msg.ack().await.unwrap();
    }
    assert_eq!(keys, vec!["k0", "k1", "k2"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_starts_after_existing_events_from_latest() {
    let (_file, db) = fresh_db();
    let writer = SqliteEventWriter::new(db.conn());
    for i in 0..3 {
        writer
            .write(&ev("acme", "/x", "thing.happened", &format!("old{i}")))
            .await
            .unwrap();
    }
    let mut cfg = config("acme");
    cfg.start_from = StartFrom::Latest;
    let reader = SqliteReader::new(db.conn(), cfg);
    let mut stream = reader.read().await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    writer
        .write(&ev("acme", "/x", "thing.happened", "new"))
        .await
        .unwrap();

    let msg = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(msg.event().key().as_str(), "new");
}

#[tokio::test]
async fn reader_starts_from_timestamp() {
    let (_file, db) = fresh_db();
    let writer = SqliteEventWriter::new(db.conn());
    writer
        .write(&ev("acme", "/x", "thing.happened", "before"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let cutoff = Utc::now();
    tokio::time::sleep(Duration::from_millis(20)).await;
    writer
        .write(&ev("acme", "/x", "thing.happened", "after"))
        .await
        .unwrap();

    let mut cfg = config("acme");
    cfg.start_from = StartFrom::Timestamp(cutoff);
    let reader = SqliteReader::new(db.conn(), cfg);
    let mut stream = reader.read().await.unwrap();
    let msg = timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(msg.event().key().as_str(), "after");
}

#[tokio::test]
async fn consumer_group_resume_after_ack() {
    let (file, db) = fresh_db();
    let writer = SqliteEventWriter::new(db.conn());
    for i in 0..4 {
        writer
            .write(&ev("acme", "/x", "thing.happened", &format!("k{i}")))
            .await
            .unwrap();
    }
    let mut cfg = config("acme");
    cfg.consumer_group_id = Some(ConsumerGroupId::new("g").unwrap());
    let reader = SqliteReader::new(db.conn(), cfg.clone());
    let mut stream = reader.read().await.unwrap();
    for expected in &["k0", "k1"] {
        let msg = timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(msg.event().key().as_str(), *expected);
        msg.ack().await.unwrap();
    }
    drop(stream);

    let db2 = SqliteDatabase::open(file.path()).unwrap();
    let reader2 = SqliteReader::new(db2.conn(), cfg);
    let mut stream2 = reader2.read().await.unwrap();
    for expected in &["k2", "k3"] {
        let msg = timeout(Duration::from_secs(2), stream2.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(msg.event().key().as_str(), *expected);
        msg.ack().await.unwrap();
    }
}

#[tokio::test]
async fn nack_does_not_advance_checkpoint() {
    let (file, db) = fresh_db();
    let writer = SqliteEventWriter::new(db.conn());
    for i in 0..2 {
        writer
            .write(&ev("acme", "/x", "thing.happened", &format!("k{i}")))
            .await
            .unwrap();
    }
    let mut cfg = config("acme");
    cfg.consumer_group_id = Some(ConsumerGroupId::new("g").unwrap());
    let reader = SqliteReader::new(db.conn(), cfg.clone());
    let mut stream = reader.read().await.unwrap();
    let msg = timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(msg.event().key().as_str(), "k0");
    msg.nack().await.unwrap();
    drop(stream);

    let db2 = SqliteDatabase::open(file.path()).unwrap();
    let reader2 = SqliteReader::new(db2.conn(), cfg);
    let mut stream2 = reader2.read().await.unwrap();
    let msg = timeout(Duration::from_secs(2), stream2.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(msg.event().key().as_str(), "k0");
}

#[tokio::test]
async fn independent_consumer_groups_get_independent_offsets() {
    let (_file, db) = fresh_db();
    let writer = SqliteEventWriter::new(db.conn());
    for i in 0..3 {
        writer
            .write(&ev("acme", "/x", "thing.happened", &format!("k{i}")))
            .await
            .unwrap();
    }

    let mut cfg_a = config("acme");
    cfg_a.consumer_group_id = Some(ConsumerGroupId::new("group-a").unwrap());
    let reader_a = SqliteReader::new(db.conn(), cfg_a.clone());
    let mut stream_a = reader_a.read().await.unwrap();
    for expected in &["k0", "k1", "k2"] {
        let msg = timeout(Duration::from_secs(2), stream_a.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(msg.event().key().as_str(), *expected);
        msg.ack().await.unwrap();
    }
    drop(stream_a);

    let mut cfg_b = config("acme");
    cfg_b.consumer_group_id = Some(ConsumerGroupId::new("group-b").unwrap());
    let reader_b = SqliteReader::new(db.conn(), cfg_b);
    let mut stream_b = reader_b.read().await.unwrap();
    let msg = timeout(Duration::from_secs(2), stream_b.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(msg.event().key().as_str(), "k0");
}

#[tokio::test]
async fn reader_filters_by_topic() {
    let (_file, db) = fresh_db();
    let writer = SqliteEventWriter::new(db.conn());
    writer
        .write(&ev("acme", "/x", "task.created", "t1"))
        .await
        .unwrap();
    writer
        .write(&ev("acme", "/x", "task.completed", "t2"))
        .await
        .unwrap();
    writer
        .write(&ev("acme", "/x", "task.created", "t3"))
        .await
        .unwrap();

    let mut cfg = config("acme");
    cfg.topics = vec![Topic::new("task.created").unwrap()];
    let reader = SqliteReader::new(db.conn(), cfg);
    let mut stream = reader.read().await.unwrap();

    let m1 = timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(m1.event().key().as_str(), "t1");
    let m2 = timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(m2.event().key().as_str(), "t3");
}

#[tokio::test]
async fn reader_filters_by_namespace() {
    let (_file, db) = fresh_db();
    let writer = SqliteEventWriter::new(db.conn());
    writer
        .write(&ev("acme", "/backend", "thing.happened", "b1"))
        .await
        .unwrap();
    writer
        .write(&ev("acme", "/frontend", "thing.happened", "f1"))
        .await
        .unwrap();
    writer
        .write(&ev("acme", "/backend/auth", "thing.happened", "b2"))
        .await
        .unwrap();

    let mut cfg = config("acme");
    cfg.namespace = Some(Namespace::new("/backend").unwrap());
    let reader = SqliteReader::new(db.conn(), cfg);
    let mut stream = reader.read().await.unwrap();

    let m1 = timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(m1.event().key().as_str(), "b1");
    let m2 = timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(m2.event().key().as_str(), "b2");
}
