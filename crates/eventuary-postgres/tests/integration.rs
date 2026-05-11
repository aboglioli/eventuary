use std::time::Duration;

use chrono::Utc;
use futures::StreamExt;
use sqlx::PgPool;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::time::timeout;

use eventuary_core::io::Writer;
use eventuary_core::{
    ConsumerGroupId, Event, EventId, Namespace, OrganizationId, Payload, StartFrom, Topic,
};
use eventuary_postgres::{PgDatabase, PgEventWriter, PgReader, PgReaderConfig};

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

fn ev(org: &str, ns: &str, topic: &str, key: &str) -> Event {
    Event::builder(org, ns, topic, Payload::from_string("payload"))
        .unwrap()
        .key(key)
        .unwrap()
        .build()
}

fn config(org: &str) -> PgReaderConfig {
    PgReaderConfig {
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
async fn write_read_roundtrip() {
    let (_c, pool) = start_postgres().await;
    let writer = PgEventWriter::new(pool.clone());
    let event = ev("acme", "/x", "thing.happened", "k0");
    writer.write(&event).await.unwrap();

    let reader = PgReader::new(pool, config("acme"));
    let mut stream = reader.read().await.unwrap();
    let msg = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(msg.event().key().expect("event has key").as_str(), "k0");
    assert_eq!(msg.event().topic().as_str(), "thing.happened");
}

#[tokio::test]
async fn reader_roundtrips_lineage_fields() {
    let (_c, pool) = start_postgres().await;
    let writer = PgEventWriter::new(pool.clone());
    let parent_id = EventId::new();
    let event = Event::builder(
        "acme",
        "/x",
        "thing.happened",
        Payload::from_string("payload"),
    )
    .unwrap()
    .key("k")
    .unwrap()
    .parent_id(parent_id)
    .correlation_id("corr")
    .unwrap()
    .causation_id("cause")
    .unwrap()
    .build();
    writer.write(&event).await.unwrap();

    let reader = PgReader::new(pool, config("acme"));
    let mut stream = reader.read().await.unwrap();
    let msg = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let event = msg.event();
    assert_eq!(event.key().map(|key| key.as_str()), Some("k"));
    assert_eq!(event.parent_id(), Some(parent_id));
    assert_eq!(event.correlation_id().map(|id| id.as_str()), Some("corr"));
    assert_eq!(event.causation_id().map(|id| id.as_str()), Some("cause"));
}

#[tokio::test]
async fn reader_streams_in_sequence_order() {
    let (_c, pool) = start_postgres().await;
    let writer = PgEventWriter::new(pool.clone());
    for i in 0..5 {
        writer
            .write(&ev("acme", "/x", "thing.happened", &format!("k{i}")))
            .await
            .unwrap();
    }
    let reader = PgReader::new(pool, config("acme"));
    let mut stream = reader.read().await.unwrap();
    let mut keys = Vec::new();
    for _ in 0..5 {
        let msg = timeout(Duration::from_secs(5), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        keys.push(
            msg.event()
                .key()
                .expect("event has key")
                .as_str()
                .to_owned(),
        );
    }
    assert_eq!(keys, vec!["k0", "k1", "k2", "k3", "k4"]);
}

#[tokio::test]
async fn consumer_group_resume_after_ack() {
    let (_c, pool) = start_postgres().await;
    let writer = PgEventWriter::new(pool.clone());
    for i in 0..4 {
        writer
            .write(&ev("acme", "/x", "thing.happened", &format!("k{i}")))
            .await
            .unwrap();
    }
    let mut cfg = config("acme");
    cfg.consumer_group_id = Some(ConsumerGroupId::new("g").unwrap());

    let reader = PgReader::new(pool.clone(), cfg.clone());
    let mut stream = reader.read().await.unwrap();
    for expected in &["k0", "k1"] {
        let msg = timeout(Duration::from_secs(5), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            msg.event().key().expect("event has key").as_str(),
            *expected
        );
        msg.ack().await.unwrap();
    }
    drop(stream);

    let reader2 = PgReader::new(pool, cfg);
    let mut stream2 = reader2.read().await.unwrap();
    for expected in &["k2", "k3"] {
        let msg = timeout(Duration::from_secs(5), stream2.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            msg.event().key().expect("event has key").as_str(),
            *expected
        );
        msg.ack().await.unwrap();
    }
}

#[tokio::test]
async fn nack_does_not_advance_checkpoint() {
    let (_c, pool) = start_postgres().await;
    let writer = PgEventWriter::new(pool.clone());
    for i in 0..2 {
        writer
            .write(&ev("acme", "/x", "thing.happened", &format!("k{i}")))
            .await
            .unwrap();
    }
    let mut cfg = config("acme");
    cfg.consumer_group_id = Some(ConsumerGroupId::new("g").unwrap());

    let reader = PgReader::new(pool.clone(), cfg.clone());
    let mut stream = reader.read().await.unwrap();
    let msg = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(msg.event().key().expect("event has key").as_str(), "k0");
    msg.nack().await.unwrap();
    drop(stream);

    let reader2 = PgReader::new(pool, cfg);
    let mut stream2 = reader2.read().await.unwrap();
    let msg = timeout(Duration::from_secs(5), stream2.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(msg.event().key().expect("event has key").as_str(), "k0");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_from_latest_skips_existing_events() {
    let (_c, pool) = start_postgres().await;
    let writer = PgEventWriter::new(pool.clone());
    for i in 0..3 {
        writer
            .write(&ev("acme", "/x", "thing.happened", &format!("old{i}")))
            .await
            .unwrap();
    }
    let mut cfg = config("acme");
    cfg.start_from = StartFrom::Latest;
    let reader = PgReader::new(pool, cfg);
    let mut stream = reader.read().await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    writer
        .write(&ev("acme", "/x", "thing.happened", "new"))
        .await
        .unwrap();

    let msg = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(msg.event().key().expect("event has key").as_str(), "new");
}

#[tokio::test]
async fn start_from_timestamp_filters_old_events() {
    let (_c, pool) = start_postgres().await;
    let writer = PgEventWriter::new(pool.clone());
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

    let mut cfg = config("acme");
    cfg.start_from = StartFrom::Timestamp(cutoff);
    let reader = PgReader::new(pool, cfg);
    let mut stream = reader.read().await.unwrap();
    let msg = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(msg.event().key().expect("event has key").as_str(), "after");
}

#[tokio::test]
async fn independent_consumer_groups() {
    let (_c, pool) = start_postgres().await;
    let writer = PgEventWriter::new(pool.clone());
    for i in 0..3 {
        writer
            .write(&ev("acme", "/x", "thing.happened", &format!("k{i}")))
            .await
            .unwrap();
    }

    let mut cfg_a = config("acme");
    cfg_a.consumer_group_id = Some(ConsumerGroupId::new("group-a").unwrap());
    let reader_a = PgReader::new(pool.clone(), cfg_a);
    let mut stream_a = reader_a.read().await.unwrap();
    for expected in &["k0", "k1", "k2"] {
        let msg = timeout(Duration::from_secs(5), stream_a.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            msg.event().key().expect("event has key").as_str(),
            *expected
        );
        msg.ack().await.unwrap();
    }
    drop(stream_a);

    let mut cfg_b = config("acme");
    cfg_b.consumer_group_id = Some(ConsumerGroupId::new("group-b").unwrap());
    let reader_b = PgReader::new(pool, cfg_b);
    let mut stream_b = reader_b.read().await.unwrap();
    let msg = timeout(Duration::from_secs(5), stream_b.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(msg.event().key().expect("event has key").as_str(), "k0");
}

#[tokio::test]
async fn topic_filter() {
    let (_c, pool) = start_postgres().await;
    let writer = PgEventWriter::new(pool.clone());
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
    let reader = PgReader::new(pool, cfg);
    let mut stream = reader.read().await.unwrap();

    let m1 = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(m1.event().key().expect("event has key").as_str(), "t1");
    let m2 = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(m2.event().key().expect("event has key").as_str(), "t3");
}

#[tokio::test]
async fn namespace_filter() {
    let (_c, pool) = start_postgres().await;
    let writer = PgEventWriter::new(pool.clone());
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
    let reader = PgReader::new(pool, cfg);
    let mut stream = reader.read().await.unwrap();

    let m1 = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(m1.event().key().expect("event has key").as_str(), "b1");
    let m2 = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(m2.event().key().expect("event has key").as_str(), "b2");
}
