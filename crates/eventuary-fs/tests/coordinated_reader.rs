use std::collections::HashSet;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use tokio::time::timeout;

use eventuary_core::io::reader::CheckpointScope;
use eventuary_core::io::{ConsumerGroupId, OwnerId, Reader, StreamId, Writer};
use eventuary_core::partition::{EventKeyPartitionKeyResolver, Fnv1a64PartitionHasher};
use eventuary_core::{Error, Event, Payload, StartFrom};
use eventuary_fs::coordinator::{FsPartitionCoordinator, FsPartitionCoordinatorConfig};
use eventuary_fs::reader::{
    FsCoordinatedReader, FsCoordinatedReaderConfig, FsCoordinatedStream, FsCoordinatedSubscription,
    FsCursor, FsReader, FsReaderConfig, FsSubscription,
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

async fn settled_group(coordinator: &Arc<FsPartitionCoordinator<FsCursor>>, want: usize) {
    use eventuary_core::io::reader::PartitionCoordinator;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if coordinator.live_consumers(&scope()).await.unwrap() >= want {
            tokio::time::sleep(Duration::from_millis(200)).await;
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("consumer group never reached {want} live consumers");
}

async fn drain(mut stream: FsCoordinatedStream) -> (HashSet<String>, HashSet<u32>) {
    let mut keys = HashSet::new();
    let mut partitions = HashSet::new();
    while let Ok(Some(Ok(message))) = timeout(Duration::from_millis(1500), stream.next()).await {
        let partition = message.cursor().partition.id();
        let key = message.event().key().as_str().to_owned();
        match message.ack().await {
            Ok(()) => {}
            Err(Error::OwnershipLost(_)) => continue,
            Err(e) => panic!("unexpected ack failure: {e}"),
        }
        keys.insert(key);
        partitions.insert(partition);
    }
    (keys, partitions)
}

#[tokio::test]
async fn two_coordinated_readers_split_partitions_without_overlap() {
    let dir = tempfile::tempdir().unwrap();
    let writer = FsWriter::open(dir.path(), writer_config()).unwrap();

    let coordinator = Arc::new(
        FsPartitionCoordinator::open(dir.path(), FsPartitionCoordinatorConfig::default()).unwrap(),
    );
    let first = coordinated(dir.path(), Arc::clone(&coordinator));
    let second = coordinated(dir.path(), Arc::clone(&coordinator));

    let stream_a = first.read(subscription(StartFrom::Earliest)).await.unwrap();
    let stream_b = second
        .read(subscription(StartFrom::Earliest))
        .await
        .unwrap();

    settled_group(&coordinator, 2).await;

    let keys: Vec<String> = (0..40).map(|i| format!("k{i}")).collect();
    for key in &keys {
        writer.write(&ev(key)).await.unwrap();
    }
    writer.sync().await.unwrap();

    let a = tokio::spawn(drain(stream_a));
    let b = tokio::spawn(drain(stream_b));
    let (keys_a, partitions_a) = a.await.unwrap();
    let (keys_b, partitions_b) = b.await.unwrap();

    let overlap: Vec<_> = partitions_a.intersection(&partitions_b).collect();
    assert!(
        overlap.is_empty(),
        "partitions must not be served by both readers: {overlap:?}"
    );

    let delivered: HashSet<&String> = keys_a.union(&keys_b).collect();
    let missing: Vec<&String> = keys.iter().filter(|k| !delivered.contains(k)).collect();
    assert!(
        missing.is_empty(),
        "the group must cover every event, missing {missing:?}"
    );
}
