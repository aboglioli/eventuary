use std::time::Duration;

use chrono::Utc;
use futures::StreamExt;
use sqlx::PgPool;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::time::timeout;

use eventuary_core::io::filter::EventFilter;
use eventuary_core::io::{Reader, Writer};
use eventuary_core::{
    Event, EventId, Namespace, NamespacePattern, OrganizationId, Payload, StartFrom, Topic,
    TopicPattern,
};
use eventuary_postgres::{
    PgCursor, PgDatabase, PgEventWriter, PgReader, PgReaderConfig, PgSubscription,
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

fn ev(org: &str, ns: &str, topic: &str, key: &str) -> Event {
    Event::builder(org, ns, topic, Payload::from_string("payload"))
        .unwrap()
        .key(key)
        .unwrap()
        .build()
        .expect("valid event")
}

fn sub_for(org: &str) -> PgSubscription {
    PgSubscription {
        start: StartFrom::Earliest,
        filter: EventFilter::for_organization(OrganizationId::new(org).unwrap()),
        batch_size: Some(10),
        limit: None,
    }
}

fn fast_config() -> PgReaderConfig {
    PgReaderConfig {
        poll_interval: Duration::from_millis(20),
        ..PgReaderConfig::default()
    }
}

#[tokio::test]
async fn write_read_roundtrip() {
    let (_c, pool) = start_postgres().await;
    let writer = PgEventWriter::new(pool.clone());
    let event = ev("acme", "/x", "thing.happened", "k0");
    writer.write(&event).await.unwrap();

    let reader = PgReader::new(pool, fast_config());
    let mut stream = reader.read(sub_for("acme")).await.unwrap();
    let msg = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(msg.event().key().expect("event has key").as_str(), "k0");
    assert!(msg.cursor().sequence() > 0);
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
    .build()
    .unwrap();
    writer.write(&event).await.unwrap();

    let reader = PgReader::new(pool, fast_config());
    let mut stream = reader.read(sub_for("acme")).await.unwrap();
    let msg = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let event = msg.event();
    assert_eq!(event.parent_id(), Some(parent_id));
    assert_eq!(event.correlation_id().map(|id| id.as_str()), Some("corr"));
    assert_eq!(event.causation_id().map(|id| id.as_str()), Some("cause"));
}

#[tokio::test]
async fn postgres_reader_advances_after_ack() {
    let (_c, pool) = start_postgres().await;
    let writer = PgEventWriter::new(pool.clone());
    writer
        .write(&ev("acme", "/x", "thing.happened", "k0"))
        .await
        .unwrap();
    writer
        .write(&ev("acme", "/x", "thing.happened", "k1"))
        .await
        .unwrap();

    let reader = PgReader::new(pool, fast_config());
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
async fn postgres_reader_redelivers_after_nack() {
    let (_c, pool) = start_postgres().await;
    let writer = PgEventWriter::new(pool.clone());
    writer
        .write(&ev("acme", "/x", "thing.happened", "k0"))
        .await
        .unwrap();

    let reader = PgReader::new(pool, fast_config());
    let mut stream = reader.read(sub_for("acme")).await.unwrap();

    let first = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let first_id = first.event().id();
    assert_eq!(first.event().key().unwrap().as_str(), "k0");
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
    let (_c, pool) = start_postgres().await;
    let writer = PgEventWriter::new(pool.clone());
    for i in 0..4 {
        writer
            .write(&ev("acme", "/x", "thing.happened", &format!("k{i}")))
            .await
            .unwrap();
    }

    let reader = PgReader::new(pool.clone(), fast_config());
    let mut stream = reader.read(sub_for("acme")).await.unwrap();
    let msg = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let cursor = *msg.cursor();
    assert_eq!(msg.event().key().unwrap().as_str(), "k0");
    msg.ack().await.unwrap();
    drop(stream);

    let resume = PgSubscription {
        start: StartFrom::After(cursor),
        filter: EventFilter::for_organization(OrganizationId::new("acme").unwrap()),
        batch_size: Some(10),
        limit: None,
    };
    let reader2 = PgReader::new(pool, fast_config());
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
    let (_c, pool) = start_postgres().await;
    let writer = PgEventWriter::new(pool.clone());
    for i in 0..3 {
        writer
            .write(&ev("acme", "/x", "thing.happened", &format!("old{i}")))
            .await
            .unwrap();
    }
    let subscription = PgSubscription {
        start: StartFrom::Latest,
        filter: EventFilter::for_organization(OrganizationId::new("acme").unwrap()),
        batch_size: Some(10),
        limit: None,
    };
    let reader = PgReader::new(pool, fast_config());
    let mut stream = reader.read(subscription).await.unwrap();

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
    assert_eq!(msg.event().key().unwrap().as_str(), "new");
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

    let subscription = PgSubscription {
        start: StartFrom::Timestamp(cutoff),
        filter: EventFilter::for_organization(OrganizationId::new("acme").unwrap()),
        batch_size: Some(10),
        limit: None,
    };
    let reader = PgReader::new(pool, fast_config());
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

    let mut filter = EventFilter::for_organization(OrganizationId::new("acme").unwrap());
    filter.topic = Some(TopicPattern::exact(Topic::new("task.created").unwrap()));
    let subscription = PgSubscription {
        start: StartFrom::Earliest,
        filter,
        batch_size: Some(10),
        limit: None,
    };
    let reader = PgReader::new(pool, fast_config());
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

    let mut filter = EventFilter::for_organization(OrganizationId::new("acme").unwrap());
    filter.namespace = Some(NamespacePattern::prefix(
        Namespace::new("/backend").unwrap(),
    ));
    let subscription = PgSubscription {
        start: StartFrom::Earliest,
        filter,
        batch_size: Some(10),
        limit: None,
    };
    let reader = PgReader::new(pool, fast_config());
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

#[allow(dead_code)]
fn _cursor_type_uses_pg_cursor() {
    let _: PgCursor = PgCursor::new(1);
}
