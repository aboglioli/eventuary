use std::time::Duration;

use futures::StreamExt;
use tokio::time::timeout;

use eventuary_core::io::CheckpointStore;
use eventuary_core::io::checkpoint::{CheckpointScope, StreamId};
use eventuary_core::io::readers::{
    CheckpointReader, CheckpointSubscription, PartitionedCursor, PartitionedReader,
    PartitionedReaderConfig, PartitionedSubscription,
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

    let store = SqliteCheckpointStore::<eventuary_sqlite::SqliteCursor>::new(
        db.conn(),
        SqliteCheckpointStoreConfig::default(),
    );
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

    let source2 = SqliteReader::new(db.conn(), fast_config());
    let store2 = SqliteCheckpointStore::<eventuary_sqlite::SqliteCursor>::new(
        db.conn(),
        SqliteCheckpointStoreConfig::default(),
    );
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
async fn checkpoint_over_partitioned_sqlite_stores_per_lane_offsets() {
    let db = SqliteDatabase::open_in_memory().unwrap();
    let writer = SqliteEventWriter::new(db.conn());
    for i in 0..6 {
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
    let store = SqliteCheckpointStore::<PartitionedCursor<eventuary_sqlite::SqliteCursor>>::new(
        db.conn(),
        SqliteCheckpointStoreConfig::default(),
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

    let store2 = SqliteCheckpointStore::<PartitionedCursor<eventuary_sqlite::SqliteCursor>>::new(
        db.conn(),
        SqliteCheckpointStoreConfig::default(),
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
async fn checkpoint_reader_no_advance_on_nack() {
    let db = SqliteDatabase::open_in_memory().unwrap();
    let writer = SqliteEventWriter::new(db.conn());
    writer
        .write(&ev("acme", "/x", "thing.happened", "k0"))
        .await
        .unwrap();

    let source = SqliteReader::new(db.conn(), fast_config());
    let store = SqliteCheckpointStore::<eventuary_sqlite::SqliteCursor>::new(
        db.conn(),
        SqliteCheckpointStoreConfig::default(),
    );
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

    let store2 = SqliteCheckpointStore::<eventuary_sqlite::SqliteCursor>::new(
        db.conn(),
        SqliteCheckpointStoreConfig::default(),
    );
    let rows = store2.load_scope(&scope()).await.unwrap();
    assert!(rows.is_empty(), "nack must not commit checkpoint");
}

// Contiguous-delivered-order ack semantics are unit-tested in
// `eventuary-core` against `PendingState` directly. An end-to-end test
// against a real source reader would require multiple in-flight messages
// per partition, but the current readers (source-cursor SQL readers and
// the lane scheduler) hold at most one in-flight per lane, so the
// scenario cannot be constructed without a synthetic test reader.

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
