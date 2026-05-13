use std::time::Duration;

use futures::StreamExt;
use tokio::time::timeout;

use eventuary_core::io::checkpoint::{CheckpointScope, StreamId};
use eventuary_core::io::readers::{
    CheckpointReader, CheckpointSubscription, PartitionedReader, PartitionedReaderConfig,
    PartitionedSubscription,
};
use eventuary_core::io::{EventFilter, Reader, Writer};
use eventuary_core::{ConsumerGroupId, Event, OrganizationId, Payload, StartFrom};
use eventuary_sqlite::{
    SqliteCheckpointStore, SqliteCheckpointStoreConfig, SqliteDatabase, SqliteEventWriter,
    SqliteReader, SqliteReaderConfig, SqliteSubscription,
};

fn ev(org: &str, ns: &str, topic: &str, key: &str) -> Event {
    Event::builder(org, ns, topic, Payload::from_string("p"))
        .unwrap()
        .key(key)
        .unwrap()
        .build()
        .expect("valid event")
}

fn fast_config() -> SqliteReaderConfig {
    SqliteReaderConfig {
        poll_interval: Duration::from_millis(10),
        ..SqliteReaderConfig::default()
    }
}

fn sub_for(org: &str) -> SqliteSubscription {
    SqliteSubscription {
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
async fn checkpoint_reader_over_sqlite_resumes_after_ack() {
    let db = SqliteDatabase::open_in_memory().unwrap();
    let writer = SqliteEventWriter::new(db.conn());
    for i in 0..3 {
        writer
            .write(&ev("acme", "/x", "thing.happened", &format!("k{i}")))
            .await
            .unwrap();
    }

    let store = SqliteCheckpointStore::new(db.conn(), SqliteCheckpointStoreConfig::default());
    let source = SqliteReader::new(db.conn(), fast_config());
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

    // brief settle so commit task drains
    tokio::time::sleep(Duration::from_millis(200)).await;

    let source2 = SqliteReader::new(db.conn(), fast_config());
    let store2 = SqliteCheckpointStore::new(db.conn(), SqliteCheckpointStoreConfig::default());
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
async fn partitioned_reader_tags_partition_on_cursor() {
    let db = SqliteDatabase::open_in_memory().unwrap();
    let writer = SqliteEventWriter::new(db.conn());
    for i in 0..8 {
        writer
            .write(&ev("acme", "/x", "thing.happened", &format!("k{i}")))
            .await
            .unwrap();
    }

    let source = SqliteReader::new(db.conn(), fast_config());
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
    let mut delivered = 0usize;
    while delivered < 8 {
        let msg = timeout(Duration::from_secs(5), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(msg.cursor().partition().id() < 4);
        msg.ack().await.unwrap();
        delivered += 1;
    }
    assert_eq!(delivered, 8);
}
