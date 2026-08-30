use std::collections::HashSet;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use tokio::time::timeout;

use eventuary_core::io::reader::CheckpointScope;
use eventuary_core::io::{ConsumerGroupId, OwnerId, Reader, StreamId, Writer};
use eventuary_core::partition::{EventKeyPartitionKeyResolver, Fnv1a64PartitionHasher};
use eventuary_core::{Event, Payload, StartFrom};
use eventuary_fs::coordinator::{FsPartitionCoordinator, FsPartitionCoordinatorConfig};
use eventuary_fs::reader::{
    FsCoordinatedReader, FsCoordinatedReaderConfig, FsCoordinatedSubscription, FsCursor, FsReader,
    FsReaderConfig, FsSubscription,
};
use eventuary_fs::writer::{FsPartitioningConfig, FsWriter, FsWriterConfig};

const PARTITIONS: u32 = 4;

fn ev(key: &str) -> Event {
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

fn scope() -> CheckpointScope {
    CheckpointScope::new(
        ConsumerGroupId::new("projection").unwrap(),
        StreamId::new("events").unwrap(),
    )
}

fn writer_config() -> FsWriterConfig {
    FsWriterConfig {
        partitioning: FsPartitioningConfig::inline(
            NonZeroU32::new(PARTITIONS).unwrap(),
            EventKeyPartitionKeyResolver::new(),
            Fnv1a64PartitionHasher,
        ),
        ..FsWriterConfig::default()
    }
}

fn coordinated(
    root: &std::path::Path,
    coordinator: Arc<FsPartitionCoordinator<FsCursor>>,
) -> FsCoordinatedReader {
    FsCoordinatedReader::new(
        FsReader::open(
            root,
            FsReaderConfig {
                poll_interval: Duration::from_millis(10),
                ..FsReaderConfig::default()
            },
        )
        .unwrap(),
        coordinator,
        OwnerId::generate(),
        FsCoordinatedReaderConfig {
            rebalance_interval: Duration::from_millis(50),
            partition_lease_duration: Duration::from_secs(10),
            ..FsCoordinatedReaderConfig::default()
        },
    )
}

fn subscription(start: StartFrom<FsCursor>) -> FsCoordinatedSubscription {
    FsCoordinatedSubscription {
        inner: FsSubscription {
            start: start.clone(),
            ..FsSubscription::default()
        },
        scope: scope(),
        partition_count: NonZeroU32::new(PARTITIONS).unwrap(),
        start,
    }
}

#[tokio::test]
async fn coordinated_reader_claims_partitions_and_delivers_every_event() {
    let dir = tempfile::tempdir().unwrap();
    let writer = FsWriter::open(dir.path(), writer_config()).unwrap();
    for key in ["k0", "k1", "k2", "k3", "k4", "k5"] {
        writer.write(&ev(key)).await.unwrap();
    }
    writer.sync().await.unwrap();

    let coordinator = Arc::new(
        FsPartitionCoordinator::open(dir.path(), FsPartitionCoordinatorConfig::default()).unwrap(),
    );
    let reader = coordinated(dir.path(), coordinator);
    let mut stream = reader
        .read(subscription(StartFrom::Earliest))
        .await
        .unwrap();

    let mut seen = HashSet::new();
    while let Ok(Some(Ok(message))) = timeout(Duration::from_secs(5), stream.next()).await {
        assert_eq!(message.cursor().partition.count(), PARTITIONS);
        assert_eq!(
            message.cursor().partition.id(),
            message.cursor().inner.inner().partition.id()
        );
        message.ack().await.unwrap();
        seen.insert(message.event().key().as_str().to_owned());
        if seen.len() == 6 {
            break;
        }
    }

    assert_eq!(seen.len(), 6);
}

#[tokio::test]
async fn coordinated_reader_registers_itself_as_a_live_consumer() {
    let dir = tempfile::tempdir().unwrap();
    let writer = FsWriter::open(dir.path(), writer_config()).unwrap();
    writer.write(&ev("k0")).await.unwrap();
    writer.sync().await.unwrap();

    let coordinator = Arc::new(
        FsPartitionCoordinator::open(dir.path(), FsPartitionCoordinatorConfig::default()).unwrap(),
    );
    let reader = coordinated(dir.path(), Arc::clone(&coordinator));
    let mut stream = reader
        .read(subscription(StartFrom::Earliest))
        .await
        .unwrap();
    let message = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    message.ack().await.unwrap();

    use eventuary_core::io::reader::PartitionCoordinator;
    assert_eq!(coordinator.live_consumers(&scope()).await.unwrap(), 1);
}

#[tokio::test]
async fn two_coordinated_readers_split_partitions_without_overlap() {
    let dir = tempfile::tempdir().unwrap();
    let writer = FsWriter::open(dir.path(), writer_config()).unwrap();
    for i in 0..40 {
        writer.write(&ev(&format!("k{i}"))).await.unwrap();
    }
    writer.sync().await.unwrap();

    let coordinator = Arc::new(
        FsPartitionCoordinator::open(dir.path(), FsPartitionCoordinatorConfig::default()).unwrap(),
    );
    let first = coordinated(dir.path(), Arc::clone(&coordinator));
    let second = coordinated(dir.path(), Arc::clone(&coordinator));

    let mut stream_a = first.read(subscription(StartFrom::Earliest)).await.unwrap();
    let mut stream_b = second
        .read(subscription(StartFrom::Earliest))
        .await
        .unwrap();

    let mut partitions_a = HashSet::new();
    let mut partitions_b = HashSet::new();
    let mut keys = HashSet::new();

    for _ in 0..40 {
        tokio::select! {
            item = timeout(Duration::from_millis(800), stream_a.next()) => {
                if let Ok(Some(Ok(message))) = item {
                    message.ack().await.unwrap();
                    partitions_a.insert(message.cursor().partition.id());
                    keys.insert(message.event().key().as_str().to_owned());
                }
            }
            item = timeout(Duration::from_millis(800), stream_b.next()) => {
                if let Ok(Some(Ok(message))) = item {
                    message.ack().await.unwrap();
                    partitions_b.insert(message.cursor().partition.id());
                    keys.insert(message.event().key().as_str().to_owned());
                }
            }
        }
        if keys.len() == 40 {
            break;
        }
    }

    let overlap: Vec<_> = partitions_a.intersection(&partitions_b).collect();
    assert!(
        overlap.is_empty(),
        "partitions must not be delivered to both readers: {overlap:?}"
    );
    assert!(!keys.is_empty());
}
