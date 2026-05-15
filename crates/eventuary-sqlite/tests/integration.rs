use std::time::Duration;

use chrono::Utc;
use futures::StreamExt;
use tokio::time::timeout;

use eventuary_core::io::filter::EventFilter;
use eventuary_core::io::{Reader, Writer};
use eventuary_core::{
    Event, EventId, Namespace, NamespacePattern, OrganizationId, Payload, StartFrom, Topic,
    TopicPattern,
};
use eventuary_sqlite::database::SqliteDatabase;
use eventuary_sqlite::reader::{SqliteReaderConfig, SqliteSubscription};
use eventuary_sqlite::{SqliteReader, SqliteWriter};

fn ev(org: &str, ns: &str, topic: &str, key: &str) -> Event {
    Event::builder(org, ns, topic, Payload::from_string("payload"))
        .unwrap()
        .key(key)
        .unwrap()
        .build()
        .expect("valid event")
}

fn sub_for(org: &str) -> SqliteSubscription {
    SqliteSubscription {
        start: StartFrom::Earliest,
        filter: EventFilter::for_organization(OrganizationId::new(org).unwrap()),
        batch_size: Some(10),
        limit: None,
    }
}

fn fast_config() -> SqliteReaderConfig {
    SqliteReaderConfig {
        poll_interval: Duration::from_millis(20),
        ..SqliteReaderConfig::default()
    }
}

#[tokio::test]
async fn write_read_roundtrip() {
    let db = SqliteDatabase::open_in_memory().unwrap();
    let writer = SqliteWriter::new(db.conn());
    writer
        .write(&ev("acme", "/x", "thing.happened", "k0"))
        .await
        .unwrap();

    let reader = SqliteReader::new(db.conn(), fast_config());
    let mut stream = reader.read(sub_for("acme")).await.unwrap();
    let msg = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(msg.event().key().unwrap().as_str(), "k0");
}

#[tokio::test]
async fn reader_roundtrips_lineage_fields() {
    let db = SqliteDatabase::open_in_memory().unwrap();
    let writer = SqliteWriter::new(db.conn());
    let parent_id = EventId::new();
    let event = Event::builder("acme", "/x", "thing.happened", Payload::from_string("p"))
        .unwrap()
        .key("k")
        .unwrap()
        .parent_id(parent_id)
        .correlation_id("corr")
        .unwrap()
        .causation_id("cause")
        .unwrap()
        .build()
        .unwrap();
    writer.write(&event).await.unwrap();

    let reader = SqliteReader::new(db.conn(), fast_config());
    let mut stream = reader.read(sub_for("acme")).await.unwrap();
    let msg = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let event = msg.event();
    assert_eq!(event.parent_id(), Some(parent_id));
    assert_eq!(event.correlation_id().map(|i| i.as_str()), Some("corr"));
    assert_eq!(event.causation_id().map(|i| i.as_str()), Some("cause"));
}

#[tokio::test]
async fn sqlite_reader_advances_after_ack() {
    let db = SqliteDatabase::open_in_memory().unwrap();
    let writer = SqliteWriter::new(db.conn());
    writer
        .write(&ev("acme", "/x", "thing.happened", "k0"))
        .await
        .unwrap();
    writer
        .write(&ev("acme", "/x", "thing.happened", "k1"))
        .await
        .unwrap();

    let reader = SqliteReader::new(db.conn(), fast_config());
    let mut stream = reader.read(sub_for("acme")).await.unwrap();

    let first = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first.event().key().unwrap().as_str(), "k0");
    first.ack().await.unwrap();

    let second = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(second.event().key().unwrap().as_str(), "k1");
}

#[tokio::test]
async fn sqlite_reader_redelivers_after_nack() {
    let db = SqliteDatabase::open_in_memory().unwrap();
    let writer = SqliteWriter::new(db.conn());
    writer
        .write(&ev("acme", "/x", "thing.happened", "k0"))
        .await
        .unwrap();

    let reader = SqliteReader::new(db.conn(), fast_config());
    let mut stream = reader.read(sub_for("acme")).await.unwrap();

    let first = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let first_id = first.event().id();
    first.nack().await.unwrap();

    let second = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(second.event().id(), first_id);
}

#[tokio::test]
async fn start_from_after_cursor_resumes() {
    let db = SqliteDatabase::open_in_memory().unwrap();
    let writer = SqliteWriter::new(db.conn());
    for i in 0..3 {
        writer
            .write(&ev("acme", "/x", "thing.happened", &format!("k{i}")))
            .await
            .unwrap();
    }
    let reader = SqliteReader::new(db.conn(), fast_config());
    let mut stream = reader.read(sub_for("acme")).await.unwrap();
    let msg = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let cursor = *msg.cursor();
    msg.ack().await.unwrap();
    drop(stream);

    let resume = SqliteSubscription {
        start: StartFrom::After(cursor),
        ..sub_for("acme")
    };
    let reader2 = SqliteReader::new(db.conn(), fast_config());
    let mut stream2 = reader2.read(resume).await.unwrap();
    let msg = timeout(Duration::from_secs(5), stream2.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(msg.event().key().unwrap().as_str(), "k1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_from_latest_skips_existing_events() {
    let db = SqliteDatabase::open_in_memory().unwrap();
    let writer = SqliteWriter::new(db.conn());
    for i in 0..3 {
        writer
            .write(&ev("acme", "/x", "thing.happened", &format!("old{i}")))
            .await
            .unwrap();
    }
    let subscription = SqliteSubscription {
        start: StartFrom::Latest,
        ..sub_for("acme")
    };
    let reader = SqliteReader::new(db.conn(), fast_config());
    let mut stream = reader.read(subscription).await.unwrap();
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
    assert_eq!(msg.event().key().unwrap().as_str(), "new");
}

#[tokio::test]
async fn start_from_timestamp_filters_old_events() {
    let db = SqliteDatabase::open_in_memory().unwrap();
    let writer = SqliteWriter::new(db.conn());
    writer
        .write(&ev("acme", "/x", "thing.happened", "before"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let cutoff = Utc::now();
    tokio::time::sleep(Duration::from_millis(50)).await;
    writer
        .write(&ev("acme", "/x", "thing.happened", "after"))
        .await
        .unwrap();

    let subscription = SqliteSubscription {
        start: StartFrom::Timestamp(cutoff),
        ..sub_for("acme")
    };
    let reader = SqliteReader::new(db.conn(), fast_config());
    let mut stream = reader.read(subscription).await.unwrap();
    let msg = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(msg.event().key().unwrap().as_str(), "after");
}

#[tokio::test]
async fn topic_filter() {
    let db = SqliteDatabase::open_in_memory().unwrap();
    let writer = SqliteWriter::new(db.conn());
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

    let mut filter = EventFilter::for_organization(OrganizationId::new("acme").unwrap());
    filter.topic = Some(TopicPattern::exact(Topic::new("task.created").unwrap()));
    let subscription = SqliteSubscription {
        start: StartFrom::Earliest,
        filter,
        batch_size: Some(10),
        limit: None,
    };
    let reader = SqliteReader::new(db.conn(), fast_config());
    let mut stream = reader.read(subscription).await.unwrap();
    let m1 = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(m1.event().key().unwrap().as_str(), "t1");
    m1.ack().await.unwrap();
    let m2 = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(m2.event().key().unwrap().as_str(), "t3");
}

#[tokio::test]
async fn namespace_filter() {
    let db = SqliteDatabase::open_in_memory().unwrap();
    let writer = SqliteWriter::new(db.conn());
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

    let mut filter = EventFilter::for_organization(OrganizationId::new("acme").unwrap());
    filter.namespace = Some(NamespacePattern::prefix(
        Namespace::new("/backend").unwrap(),
    ));
    let subscription = SqliteSubscription {
        start: StartFrom::Earliest,
        filter,
        batch_size: Some(10),
        limit: None,
    };
    let reader = SqliteReader::new(db.conn(), fast_config());
    let mut stream = reader.read(subscription).await.unwrap();
    let m1 = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(m1.event().key().unwrap().as_str(), "b1");
    m1.ack().await.unwrap();
    let m2 = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(m2.event().key().unwrap().as_str(), "b2");
}
