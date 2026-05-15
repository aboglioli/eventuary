use std::time::Duration;

use futures::StreamExt;
use sqlx::PgPool;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::time::timeout;

use eventuary_core::io::ConsumerGroupId;
use eventuary_core::io::filter::EventFilter;
use eventuary_core::io::reader::{
    CheckpointReader, CheckpointScope, CheckpointStore, CheckpointSubscription, PartitionedCursor,
    PartitionedReader, PartitionedReaderConfig, PartitionedSubscription,
};
use eventuary_core::io::{Reader, StreamId, Writer};
use eventuary_core::{Event, OrganizationId, Payload, StartFrom};
use eventuary_postgres::checkpoint_store::{PgCheckpointStore, PgCheckpointStoreConfig};
use eventuary_postgres::database::PgDatabase;
use eventuary_postgres::reader::{PgCursor, PgReader, PgReaderConfig, PgSubscription};
use eventuary_postgres::writer::PgWriter;

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
    Event::builder(org, ns, topic, Payload::from_string("p"))
        .unwrap()
        .key(key)
        .unwrap()
        .build()
        .expect("valid event")
}

fn fast_config() -> PgReaderConfig {
    PgReaderConfig {
        poll_interval: Duration::from_millis(20),
        ..PgReaderConfig::default()
    }
}

fn sub_for(org: &str) -> PgSubscription {
    PgSubscription {
        start: StartFrom::Earliest,
        filter: EventFilter::for_organization(OrganizationId::new(org).unwrap()),
        batch_size: Some(10),
        limit: None,
    }
}

fn scope() -> CheckpointScope {
    CheckpointScope::new(
        ConsumerGroupId::new("workers").unwrap(),
        StreamId::new("billing").unwrap(),
    )
}

#[tokio::test]
async fn checkpoint_reader_over_pg_reader_resumes_after_ack() {
    let (_c, pool) = start_postgres().await;
    let writer = PgWriter::new(pool.clone());
    for i in 0..3 {
        writer
            .write(&ev("acme", "/x", "thing.happened", &format!("k{i}")))
            .await
            .unwrap();
    }

    let store =
        PgCheckpointStore::<PgCursor>::new(pool.clone(), PgCheckpointStoreConfig::default());
    let source = PgReader::new(pool.clone(), fast_config());
    let checkpointed = CheckpointReader::new(source, store);

    let mut stream = checkpointed
        .read(CheckpointSubscription::new(sub_for("acme"), scope()))
        .await
        .unwrap();
    let m0 = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(m0.event().key().unwrap().as_str(), "k0");
    m0.ack().await.unwrap();
    let m1 = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(m1.event().key().unwrap().as_str(), "k1");
    m1.ack().await.unwrap();
    drop(stream);

    let source2 = PgReader::new(pool.clone(), fast_config());
    let store2 = PgCheckpointStore::<PgCursor>::new(pool, PgCheckpointStoreConfig::default());
    let checkpointed2 = CheckpointReader::new(source2, store2);
    let mut stream2 = checkpointed2
        .read(CheckpointSubscription::new(sub_for("acme"), scope()))
        .await
        .unwrap();
    let next = timeout(Duration::from_secs(5), stream2.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(next.event().key().unwrap().as_str(), "k2");
}

#[tokio::test]
async fn checkpoint_reader_over_pg_reader_no_advance_on_nack() {
    let (_c, pool) = start_postgres().await;
    let writer = PgWriter::new(pool.clone());
    writer
        .write(&ev("acme", "/x", "thing.happened", "k0"))
        .await
        .unwrap();

    let source = PgReader::new(pool.clone(), fast_config());
    let store =
        PgCheckpointStore::<PgCursor>::new(pool.clone(), PgCheckpointStoreConfig::default());
    let checkpointed = CheckpointReader::new(source, store);

    let mut stream = checkpointed
        .read(CheckpointSubscription::new(sub_for("acme"), scope()))
        .await
        .unwrap();
    let m0 = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    m0.nack().await.unwrap();
    drop(stream);

    let store2 = PgCheckpointStore::<PgCursor>::new(pool, PgCheckpointStoreConfig::default());
    let rows = store2.load_scope(&scope()).await.unwrap();
    assert!(rows.is_empty(), "nack must not commit checkpoint");
}

#[tokio::test]
async fn checkpoint_over_partitioned_pg_reader_stores_per_lane_offsets() {
    let (_c, pool) = start_postgres().await;
    let writer = PgWriter::new(pool.clone());
    for i in 0..6 {
        writer
            .write(&ev("acme", "/x", "thing.happened", &format!("k{i}")))
            .await
            .unwrap();
    }

    let source = PgReader::new(pool.clone(), fast_config());
    let partitioned = PartitionedReader::new(
        source,
        PartitionedReaderConfig {
            partition_count: std::num::NonZeroU16::new(4).unwrap(),
            ..PartitionedReaderConfig::default()
        },
    );
    let store = PgCheckpointStore::<PartitionedCursor<PgCursor>>::new(
        pool.clone(),
        PgCheckpointStoreConfig::default(),
    );
    let checkpointed = CheckpointReader::new(partitioned, store);

    let inner = PartitionedSubscription::new(sub_for("acme"));
    let mut stream = checkpointed
        .read(CheckpointSubscription::new(inner, scope()))
        .await
        .unwrap();
    for _ in 0..6 {
        let msg = timeout(Duration::from_secs(5), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        msg.ack().await.unwrap();
    }
    drop(stream);

    let store2 = PgCheckpointStore::<PartitionedCursor<PgCursor>>::new(
        pool,
        PgCheckpointStoreConfig::default(),
    );
    let rows = store2.load_scope(&scope()).await.unwrap();
    assert!(!rows.is_empty(), "expected per-lane checkpoints persisted");
    for (cursor_id, _cursor) in &rows {
        assert!(
            matches!(cursor_id, eventuary_core::io::CursorId::Named(_)),
            "partitioned cursor must be tagged with a named cursor id"
        );
    }
}

#[tokio::test]
async fn partitioned_pg_reader_continues_other_lanes_when_one_lane_unacked() {
    let (_c, pool) = start_postgres().await;
    let writer = PgWriter::new(pool.clone());
    for i in 0..16 {
        writer
            .write(&ev("acme", "/x", "thing.happened", &format!("k{i}")))
            .await
            .unwrap();
    }

    let source = PgReader::new(pool.clone(), fast_config());
    let partitioned = PartitionedReader::new(
        source,
        PartitionedReaderConfig {
            partition_count: std::num::NonZeroU16::new(4).unwrap(),
            ..PartitionedReaderConfig::default()
        },
    );

    let mut stream = partitioned
        .read(PartitionedSubscription::new(sub_for("acme")))
        .await
        .unwrap();
    let mut held = Vec::new();
    let mut lanes_seen = std::collections::HashSet::new();
    for _ in 0..4 {
        let msg = timeout(Duration::from_secs(5), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        lanes_seen.insert(msg.cursor().partition().id());
        held.push(msg);
    }
    assert!(
        lanes_seen.len() >= 2,
        "expected multiple lanes to emit without acks, got {lanes_seen:?}"
    );
    for m in held {
        m.ack().await.unwrap();
    }
}
