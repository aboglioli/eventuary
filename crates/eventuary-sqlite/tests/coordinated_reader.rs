use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use tokio::time::timeout;

use eventuary_core::io::reader::CheckpointScope;
use eventuary_core::io::{ConsumerGroupId, OwnerId, Reader, StreamId, Writer};
use eventuary_core::partition::{EventKeyPartitionKeyResolver, Fnv1a64PartitionHasher};
use eventuary_core::{Event, Payload, StartFrom};
use eventuary_sqlite::database::SqliteDatabase;
use eventuary_sqlite::partition_coordinator::{
    SqlitePartitionCoordinator, SqlitePartitionCoordinatorConfig,
};
use eventuary_sqlite::reader::{SqliteReader, SqliteReaderConfig, SqliteSubscription};
use eventuary_sqlite::writer::{SqlitePartitioningConfig, SqliteWriter, SqliteWriterConfig};
use eventuary_sqlite::{
    SqliteCoordinatedReader, SqliteCoordinatedReaderConfig, SqliteCoordinatedSubscription,
};

fn event_with_key(key: &str) -> Event {
    Event::builder(
        "acme",
        "/orders",
        "order.placed",
        Payload::from_string("{}"),
    )
    .unwrap()
    .key(key)
    .unwrap()
    .build()
    .unwrap()
}

#[tokio::test]
async fn sqlite_coordinated_reader_claims_and_delivers_partition_events() {
    let partition_count = NonZeroU16::new(4).unwrap();

    let db = SqliteDatabase::open_in_memory().unwrap();

    let writer = SqliteWriter::new_with_config(
        db.conn(),
        SqliteWriterConfig {
            partitioning: SqlitePartitioningConfig::inline(
                partition_count,
                EventKeyPartitionKeyResolver::event_id_on_unkeyed(),
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

    let writer = SqliteWriter::new_with_config(
        db.conn(),
        SqliteWriterConfig {
            partitioning: SqlitePartitioningConfig::inline(
                partition_count,
                EventKeyPartitionKeyResolver::event_id_on_unkeyed(),
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
        let key = msg.event().key().map(|k| k.as_str().to_owned()).unwrap();
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

    let writer = SqliteWriter::new_with_config(
        db.conn(),
        SqliteWriterConfig {
            partitioning: SqlitePartitioningConfig::inline(
                partition_count,
                EventKeyPartitionKeyResolver::event_id_on_unkeyed(),
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
        let key = msg.event().key().map(|k| k.as_str().to_owned()).unwrap();
        msg.ack().await.unwrap();
        delivered.push(key);
        if delivered.len() == 1 {
            break;
        }
    }

    assert_eq!(delivered, vec!["new-after-cutoff".to_owned()]);
}
