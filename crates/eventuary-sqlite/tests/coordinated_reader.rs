use std::collections::HashSet;
use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use tokio::time::timeout;

use eventuary_core::io::reader::CheckpointScope;
use eventuary_core::io::{ConsumerGroupId, OwnerId, Reader, StreamId, Writer};
use eventuary_core::partition::{EventKeyPartitionKeyResolver, Fnv1a64PartitionHasher};
use eventuary_core::{Event, Payload, StartFrom};
use eventuary_sqlite::database::{SqliteConn, SqliteDatabase};
use eventuary_sqlite::partition_coordinator::{
    SqlitePartitionCoordinator, SqlitePartitionCoordinatorConfig,
};
use eventuary_sqlite::reader::{SqliteReader, SqliteReaderConfig, SqliteSubscription};
use eventuary_sqlite::writer::{SqlitePartitioningConfig, SqliteWriter, SqliteWriterConfig};
use eventuary_sqlite::{
    SqliteCoordinatedReader, SqliteCoordinatedReaderConfig, SqliteCoordinatedSubscription,
};

fn prepare_test_schema(conn: &SqliteConn) {
    SqliteWriter::prepare_schema(conn, &SqliteWriterConfig::default()).unwrap();
    SqlitePartitionCoordinator::prepare_schema(conn, &SqlitePartitionCoordinatorConfig::default())
        .unwrap();
}

fn event_with_key(key: &str) -> Event {
    Event::builder(
        "acme",
        "/orders",
        "order.placed",
        key,
        Payload::from_string("{}"),
    )
    .unwrap()
    .build()
    .unwrap()
}

#[tokio::test]
async fn sqlite_coordinated_reader_claims_and_delivers_partition_events() {
    let partition_count = NonZeroU16::new(4).unwrap();

    let db = SqliteDatabase::open_in_memory().unwrap();
    prepare_test_schema(&db.conn());

    let writer = SqliteWriter::new_with_config(
        db.conn(),
        SqliteWriterConfig {
            partitioning: SqlitePartitioningConfig::inline(
                partition_count,
                EventKeyPartitionKeyResolver::new(),
                Fnv1a64PartitionHasher,
            ),
            ..SqliteWriterConfig::default()
        },
    );

    for key in ["k0", "k1", "k2", "k3"] {
        writer.write(&event_with_key(key)).await.unwrap();
    }

    let coordinator = Arc::new(SqlitePartitionCoordinator::new(
        db.conn(),
        SqlitePartitionCoordinatorConfig::default(),
    ));

    let reader = SqliteCoordinatedReader::new(
        SqliteReader::new(
            db.conn(),
            SqliteReaderConfig {
                poll_interval: Duration::from_millis(20),
                ..SqliteReaderConfig::default()
            },
        ),
        Arc::clone(&coordinator),
        OwnerId::generate(),
        SqliteCoordinatedReaderConfig {
            rebalance_interval: Duration::from_millis(100),
            partition_lease_duration: Duration::from_secs(10),
            ..SqliteCoordinatedReaderConfig::default()
        },
    );

    let subscription = SqliteCoordinatedSubscription {
        inner: SqliteSubscription {
            start: StartFrom::Earliest,
            ..SqliteSubscription::default()
        },
        scope: CheckpointScope::new(
            ConsumerGroupId::new("sqlite-projection").unwrap(),
            StreamId::new("sqlite-events").unwrap(),
        ),
        partition_count,
        start: StartFrom::Earliest,
    };

    let mut stream = reader.read(subscription).await.unwrap();

    let mut count = 0usize;
    while let Ok(Some(Ok(msg))) = timeout(Duration::from_secs(5), stream.next()).await {
        assert_eq!(
            msg.cursor().partition.count(),
            partition_count.get(),
            "expected partition count to match"
        );
        msg.ack().await.unwrap();
        count += 1;
        if count == 4 {
            break;
        }
    }

    assert_eq!(count, 4, "expected all 4 events to be delivered");
}

#[tokio::test]
async fn sqlite_coordinated_reader_fresh_latest_skips_existing_events() {
    let partition_count = NonZeroU16::new(4).unwrap();
    let db = SqliteDatabase::open_in_memory().unwrap();
    prepare_test_schema(&db.conn());

    let writer = SqliteWriter::new_with_config(
        db.conn(),
        SqliteWriterConfig {
            partitioning: SqlitePartitioningConfig::inline(
                partition_count,
                EventKeyPartitionKeyResolver::new(),
                Fnv1a64PartitionHasher,
            ),
            ..SqliteWriterConfig::default()
        },
    );

    writer.write(&event_with_key("old-1")).await.unwrap();
    writer.write(&event_with_key("old-2")).await.unwrap();

    let coordinator = Arc::new(SqlitePartitionCoordinator::new(
        db.conn(),
        SqlitePartitionCoordinatorConfig::default(),
    ));

    let reader = SqliteCoordinatedReader::new(
        SqliteReader::new(
            db.conn(),
            SqliteReaderConfig {
                poll_interval: Duration::from_millis(20),
                ..SqliteReaderConfig::default()
            },
        ),
        Arc::clone(&coordinator),
        OwnerId::new("fresh-latest-owner").unwrap(),
        SqliteCoordinatedReaderConfig {
            rebalance_interval: Duration::from_millis(50),
            partition_lease_duration: Duration::from_secs(10),
            partition_slack: 0,
            ..SqliteCoordinatedReaderConfig::default()
        },
    );

    let subscription = SqliteCoordinatedSubscription {
        inner: SqliteSubscription::default(),
        scope: CheckpointScope::new(
            ConsumerGroupId::new("fresh-latest-group").unwrap(),
            StreamId::new("sqlite-events").unwrap(),
        ),
        partition_count,
        start: StartFrom::Latest,
    };

    let mut stream = reader.read(subscription).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    writer.write(&event_with_key("new-1")).await.unwrap();

    let mut delivered = Vec::new();
    while let Ok(Some(Ok(msg))) = timeout(Duration::from_secs(3), stream.next()).await {
        let key = msg.event().key().as_str().to_owned();
        msg.ack().await.unwrap();
        delivered.push(key);
        if delivered.len() == 1 {
            break;
        }
    }

    assert_eq!(delivered, vec!["new-1".to_owned()]);
}

#[tokio::test]
async fn sqlite_coordinated_reader_fresh_timestamp_skips_pre_cutoff_events() {
    let partition_count = NonZeroU16::new(4).unwrap();
    let db = SqliteDatabase::open_in_memory().unwrap();
    prepare_test_schema(&db.conn());

    let writer = SqliteWriter::new_with_config(
        db.conn(),
        SqliteWriterConfig {
            partitioning: SqlitePartitioningConfig::inline(
                partition_count,
                EventKeyPartitionKeyResolver::new(),
                Fnv1a64PartitionHasher,
            ),
            ..SqliteWriterConfig::default()
        },
    );

    writer
        .write(&event_with_key("old-before-cutoff"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let cutoff = chrono::Utc::now();
    tokio::time::sleep(Duration::from_millis(20)).await;
    writer
        .write(&event_with_key("new-after-cutoff"))
        .await
        .unwrap();

    let coordinator = Arc::new(SqlitePartitionCoordinator::new(
        db.conn(),
        SqlitePartitionCoordinatorConfig::default(),
    ));

    let reader = SqliteCoordinatedReader::new(
        SqliteReader::new(
            db.conn(),
            SqliteReaderConfig {
                poll_interval: Duration::from_millis(20),
                ..SqliteReaderConfig::default()
            },
        ),
        Arc::clone(&coordinator),
        OwnerId::new("fresh-timestamp-owner").unwrap(),
        SqliteCoordinatedReaderConfig {
            rebalance_interval: Duration::from_millis(50),
            partition_lease_duration: Duration::from_secs(10),
            partition_slack: 0,
            ..SqliteCoordinatedReaderConfig::default()
        },
    );

    let subscription = SqliteCoordinatedSubscription {
        inner: SqliteSubscription::default(),
        scope: CheckpointScope::new(
            ConsumerGroupId::new("fresh-timestamp-group").unwrap(),
            StreamId::new("sqlite-events").unwrap(),
        ),
        partition_count,
        start: StartFrom::Timestamp(cutoff),
    };

    let mut stream = reader.read(subscription).await.unwrap();

    let mut delivered = Vec::new();
    while let Ok(Some(Ok(msg))) = timeout(Duration::from_secs(3), stream.next()).await {
        let key = msg.event().key().as_str().to_owned();
        msg.ack().await.unwrap();
        delivered.push(key);
        if delivered.len() == 1 {
            break;
        }
    }

    assert_eq!(delivered, vec!["new-after-cutoff".to_owned()]);
}

#[tokio::test]
async fn sqlite_coordinated_reader_persists_checkpoint_on_ack() {
    let partition_count = NonZeroU16::new(4).unwrap();

    let db = SqliteDatabase::open_in_memory().unwrap();
    prepare_test_schema(&db.conn());

    let writer = SqliteWriter::new_with_config(
        db.conn(),
        SqliteWriterConfig {
            partitioning: SqlitePartitioningConfig::inline(
                partition_count,
                EventKeyPartitionKeyResolver::new(),
                Fnv1a64PartitionHasher,
            ),
            ..SqliteWriterConfig::default()
        },
    );

    for key in ["k0", "k1", "k2", "k3"] {
        writer.write(&event_with_key(key)).await.unwrap();
    }

    let coordinator = Arc::new(SqlitePartitionCoordinator::new(
        db.conn(),
        SqlitePartitionCoordinatorConfig::default(),
    ));

    let scope = CheckpointScope::new(
        ConsumerGroupId::new("checkpoint-test").unwrap(),
        StreamId::new("orders").unwrap(),
    );

    let reader = SqliteCoordinatedReader::new(
        SqliteReader::new(
            db.conn(),
            SqliteReaderConfig {
                poll_interval: Duration::from_millis(20),
                ..SqliteReaderConfig::default()
            },
        ),
        Arc::clone(&coordinator),
        OwnerId::new("owner-1").unwrap(),
        SqliteCoordinatedReaderConfig {
            rebalance_interval: Duration::from_millis(100),
            partition_lease_duration: Duration::from_secs(10),
            ..SqliteCoordinatedReaderConfig::default()
        },
    );

    let sub = SqliteCoordinatedSubscription {
        scope: scope.clone(),
        partition_count,
        start: StartFrom::Earliest,
        inner: SqliteSubscription::default(),
    };

    let mut stream = reader.read(sub).await.unwrap();

    for _ in 0..4 {
        let msg = timeout(Duration::from_secs(5), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        msg.ack().await.unwrap();
    }

    // Stream drop triggers release, but checkpoints are already committed
    // Query the partitions table to verify checkpoints were persisted
    let conn = db.conn();
    let guard = conn.lock().unwrap();
    let mut stmt = guard
        .prepare(
            "SELECT checkpoint_sequence FROM event_stream_partitions \
                   WHERE consumer_group_id = ?1 AND stream_id = ?2",
        )
        .unwrap();
    let checkpoints: Vec<i64> = stmt
        .query_map(rusqlite::params!["checkpoint-test", "orders"], |r| {
            r.get::<_, i64>(0)
        })
        .unwrap()
        .filter_map(|r| r.ok())
        .collect();
    drop(stmt);
    drop(guard);

    assert!(
        !checkpoints.is_empty(),
        "expected at least one checkpoint entry"
    );
    assert!(
        checkpoints.iter().all(|s| *s > 0),
        "all partitions should have checkpoint_sequence > 0; got {checkpoints:?}"
    );
}

#[tokio::test]
async fn sqlite_coordinated_reader_resumes_from_checkpoint_on_restart() {
    let partition_count = NonZeroU16::new(4).unwrap();

    let db = SqliteDatabase::open_in_memory().unwrap();
    prepare_test_schema(&db.conn());

    let writer = SqliteWriter::new_with_config(
        db.conn(),
        SqliteWriterConfig {
            partitioning: SqlitePartitioningConfig::inline(
                partition_count,
                EventKeyPartitionKeyResolver::new(),
                Fnv1a64PartitionHasher,
            ),
            ..SqliteWriterConfig::default()
        },
    );

    let keys = [
        "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta",
    ];
    for key in &keys {
        writer.write(&event_with_key(key)).await.unwrap();
    }

    let coordinator = Arc::new(SqlitePartitionCoordinator::new(
        db.conn(),
        SqlitePartitionCoordinatorConfig::default(),
    ));

    let scope = CheckpointScope::new(
        ConsumerGroupId::new("resume-group").unwrap(),
        StreamId::new("orders").unwrap(),
    );

    let reader_config = SqliteCoordinatedReaderConfig {
        partition_lease_duration: Duration::from_secs(60),
        partition_renew_interval: Duration::from_secs(15),
        consumer_lease_duration: Duration::from_secs(30),
        consumer_heartbeat_interval: Duration::from_secs(10),
        rebalance_interval: Duration::from_millis(100),
        partition_slack: 0,
    };

    let make_sub = || SqliteCoordinatedSubscription {
        scope: scope.clone(),
        partition_count,
        start: StartFrom::Earliest,
        inner: SqliteSubscription::default(),
    };

    let mut acked_keys: Vec<String> = Vec::new();

    {
        let reader = SqliteCoordinatedReader::new(
            SqliteReader::new(
                db.conn(),
                SqliteReaderConfig {
                    poll_interval: Duration::from_millis(20),
                    ..SqliteReaderConfig::default()
                },
            ),
            Arc::clone(&coordinator),
            OwnerId::new("owner-1").unwrap(),
            reader_config,
        );

        let mut stream = reader.read(make_sub()).await.unwrap();

        for _ in 0..4 {
            let msg = timeout(Duration::from_secs(5), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            acked_keys.push(msg.event().key().as_str().to_owned());
            msg.ack().await.unwrap();
        }
    }

    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut resumed_keys: Vec<String> = Vec::new();

    {
        let reader2 = SqliteCoordinatedReader::new(
            SqliteReader::new(
                db.conn(),
                SqliteReaderConfig {
                    poll_interval: Duration::from_millis(20),
                    ..SqliteReaderConfig::default()
                },
            ),
            Arc::clone(&coordinator),
            OwnerId::new("owner-2").unwrap(),
            reader_config,
        );

        let mut stream2 = reader2.read(make_sub()).await.unwrap();

        while let Ok(Some(Ok(msg))) = timeout(Duration::from_secs(5), stream2.next()).await {
            resumed_keys.push(msg.event().key().as_str().to_owned());
            msg.ack().await.unwrap();
            if resumed_keys.len() >= 4 {
                break;
            }
        }
    }

    let acked_set: HashSet<&String> = acked_keys.iter().collect();
    let resumed_set: HashSet<&String> = resumed_keys.iter().collect();
    let overlap: HashSet<_> = acked_set.intersection(&resumed_set).collect();
    assert!(
        overlap.is_empty(),
        "resumed reader should not re-deliver already-acked events; overlap={overlap:?}"
    );

    let all_keys: HashSet<&str> = keys.iter().copied().collect();
    let mut combined: HashSet<String> = acked_keys.into_iter().collect();
    combined.extend(resumed_keys);
    assert_eq!(
        combined.len(),
        8,
        "combined total should be 8; got {}",
        combined.len()
    );
    for key in &all_keys {
        assert!(
            combined.contains(*key),
            "missing event key={key}; combined={combined:?}"
        );
    }
}
