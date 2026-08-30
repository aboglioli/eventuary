use std::num::NonZeroU32;
use std::time::Duration;

use futures::StreamExt;
use tokio::time::timeout;

use eventuary_core::io::filter::{EventFilter, TopicPattern};
use eventuary_core::io::{Reader, Writer};
use eventuary_core::partition::{
    EventKeyPartitionKeyResolver, Fnv1a64PartitionHasher, Partition, PartitionGroup,
    PartitionSelection,
};
use eventuary_core::{Event, OrganizationId, Payload, StartFrom, StopAt, Topic};
use eventuary_fs::reader::{FsCursor, FsReader, FsReaderConfig, FsSubscription};
use eventuary_fs::writer::{FsPartitioningConfig, FsWriter, FsWriterConfig};

fn ev(topic: &str, key: &str) -> Event {
    Event::builder("acme", "/orders", topic, key, Payload::from_string("p"))
        .unwrap()
        .build()
        .unwrap()
}

fn org_ev(org: &str, topic: &str, key: &str) -> Event {
    Event::builder(org, "/orders", topic, key, Payload::from_string("p"))
        .unwrap()
        .build()
        .unwrap()
}

fn partitioned(count: u32) -> FsWriterConfig {
    FsWriterConfig {
        partitioning: FsPartitioningConfig::inline(
            NonZeroU32::new(count).unwrap(),
            EventKeyPartitionKeyResolver::new(),
            Fnv1a64PartitionHasher,
        ),
        ..FsWriterConfig::default()
    }
}

fn fast() -> FsReaderConfig {
    FsReaderConfig {
        poll_interval: Duration::from_millis(5),
        ..FsReaderConfig::default()
    }
}

async fn collect(
    reader: &FsReader,
    subscription: FsSubscription,
    want: usize,
) -> Vec<(String, FsCursor)> {
    let mut stream = reader.read(subscription).await.unwrap();
    let mut out = Vec::new();
    while out.len() < want {
        let next = timeout(Duration::from_secs(5), stream.next()).await;
        match next {
            Ok(Some(Ok(message))) => {
                message.ack().await.unwrap();
                out.push((message.event().key().as_str().to_owned(), *message.cursor()));
            }
            Ok(Some(Err(e))) => panic!("stream error: {e}"),
            Ok(None) => break,
            Err(_) => break,
        }
    }
    out
}

async fn drain(reader: &FsReader, subscription: FsSubscription) -> Vec<String> {
    let mut stream = reader.read(subscription).await.unwrap();
    let mut out = Vec::new();
    while let Ok(Some(item)) = timeout(Duration::from_secs(5), stream.next()).await {
        let message = item.unwrap();
        message.ack().await.unwrap();
        out.push(message.event().key().as_str().to_owned());
    }
    out
}

#[tokio::test]
async fn writes_are_read_back_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let writer = FsWriter::open(dir.path(), FsWriterConfig::default()).unwrap();
    for i in 0..5 {
        writer
            .write(&ev("order.created", &format!("k{i}")))
            .await
            .unwrap();
    }
    writer.sync().await.unwrap();

    let reader = FsReader::open(dir.path(), fast()).unwrap();
    let keys = drain(
        &reader,
        FsSubscription {
            stop_at: StopAt::CurrentEnd,
            ..FsSubscription::earliest()
        },
    )
    .await;

    assert_eq!(keys, vec!["k0", "k1", "k2", "k3", "k4"]);
}

#[tokio::test]
async fn write_all_persists_every_event() {
    let dir = tempfile::tempdir().unwrap();
    let writer = FsWriter::open(dir.path(), partitioned(4)).unwrap();
    let events: Vec<Event> = (0..20)
        .map(|i| ev("order.created", &format!("k{i}")))
        .collect();

    writer.write_all(&events).await.unwrap();

    let reader = FsReader::open(dir.path(), fast()).unwrap();
    let keys = drain(
        &reader,
        FsSubscription {
            stop_at: StopAt::CurrentEnd,
            ..FsSubscription::earliest()
        },
    )
    .await;

    assert_eq!(keys.len(), 20);
}

#[tokio::test]
async fn same_key_always_lands_in_the_same_partition() {
    let dir = tempfile::tempdir().unwrap();
    let writer = FsWriter::open(dir.path(), partitioned(8)).unwrap();
    for _ in 0..6 {
        writer
            .write(&ev("order.created", "stable-key"))
            .await
            .unwrap();
    }
    writer.sync().await.unwrap();

    let reader = FsReader::open(dir.path(), fast()).unwrap();
    let read = collect(
        &reader,
        FsSubscription {
            stop_at: StopAt::CurrentEnd,
            ..FsSubscription::earliest()
        },
        6,
    )
    .await;

    let partitions: Vec<u32> = read.iter().map(|(_, c)| c.partition.id()).collect();
    assert_eq!(read.len(), 6);
    assert!(partitions.windows(2).all(|w| w[0] == w[1]));
}

#[tokio::test]
async fn offsets_are_contiguous_within_a_partition() {
    let dir = tempfile::tempdir().unwrap();
    let writer = FsWriter::open(dir.path(), FsWriterConfig::default()).unwrap();
    for i in 0..6 {
        writer
            .write(&ev("order.created", &format!("k{i}")))
            .await
            .unwrap();
    }
    writer.sync().await.unwrap();

    let reader = FsReader::open(dir.path(), fast()).unwrap();
    let read = collect(
        &reader,
        FsSubscription {
            stop_at: StopAt::CurrentEnd,
            ..FsSubscription::earliest()
        },
        6,
    )
    .await;

    let offsets: Vec<u64> = read.iter().map(|(_, c)| c.offset).collect();
    assert_eq!(offsets, vec![0, 1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn start_latest_skips_existing_events_and_tails_new_ones() {
    let dir = tempfile::tempdir().unwrap();
    let writer = FsWriter::open(dir.path(), FsWriterConfig::default()).unwrap();
    writer.write(&ev("order.created", "old")).await.unwrap();
    writer.sync().await.unwrap();

    let reader = FsReader::open(dir.path(), fast()).unwrap();
    let mut stream = reader.read(FsSubscription::default()).await.unwrap();

    writer.write(&ev("order.created", "new")).await.unwrap();
    writer.sync().await.unwrap();

    let message = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert_eq!(message.event().key().as_str(), "new");
}

#[tokio::test]
async fn start_after_cursor_resumes_at_the_next_offset() {
    let dir = tempfile::tempdir().unwrap();
    let writer = FsWriter::open(dir.path(), FsWriterConfig::default()).unwrap();
    for i in 0..5 {
        writer
            .write(&ev("order.created", &format!("k{i}")))
            .await
            .unwrap();
    }
    writer.sync().await.unwrap();
    let reader = FsReader::open(dir.path(), fast()).unwrap();
    let first = collect(
        &reader,
        FsSubscription {
            stop_at: StopAt::CurrentEnd,
            ..FsSubscription::earliest()
        },
        5,
    )
    .await;
    let third = first[2].1;

    let resumed = drain(
        &reader,
        FsSubscription {
            start: StartFrom::After(third),
            stop_at: StopAt::CurrentEnd,
            ..FsSubscription::default()
        },
    )
    .await;

    assert_eq!(resumed, vec!["k3", "k4"]);
}

#[tokio::test]
async fn start_from_timestamp_skips_earlier_events() {
    let dir = tempfile::tempdir().unwrap();
    let writer = FsWriter::open(dir.path(), FsWriterConfig::default()).unwrap();
    writer.write(&ev("order.created", "before")).await.unwrap();
    writer.sync().await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let boundary = chrono::Utc::now();
    tokio::time::sleep(Duration::from_millis(20)).await;
    writer.write(&ev("order.created", "after")).await.unwrap();
    writer.sync().await.unwrap();

    let reader = FsReader::open(dir.path(), fast()).unwrap();
    let keys = drain(
        &reader,
        FsSubscription {
            start: StartFrom::Timestamp(boundary),
            stop_at: StopAt::CurrentEnd,
            ..FsSubscription::default()
        },
    )
    .await;

    assert_eq!(keys, vec!["after"]);
}

#[tokio::test]
async fn stop_at_current_end_terminates_the_stream() {
    let dir = tempfile::tempdir().unwrap();
    let writer = FsWriter::open(dir.path(), FsWriterConfig::default()).unwrap();
    for i in 0..3 {
        writer
            .write(&ev("order.created", &format!("k{i}")))
            .await
            .unwrap();
    }
    writer.sync().await.unwrap();

    let reader = FsReader::open(dir.path(), fast()).unwrap();
    let mut stream = reader
        .read(FsSubscription {
            stop_at: StopAt::CurrentEnd,
            ..FsSubscription::earliest()
        })
        .await
        .unwrap();
    let mut count = 0;
    while let Some(item) = stream.next().await {
        item.unwrap().ack().await.unwrap();
        count += 1;
    }

    assert_eq!(count, 3);
}

#[tokio::test]
async fn filter_excludes_non_matching_events() {
    let dir = tempfile::tempdir().unwrap();
    let writer = FsWriter::open(dir.path(), FsWriterConfig::default()).unwrap();
    writer.write(&ev("order.created", "a")).await.unwrap();
    writer.write(&ev("order.shipped", "b")).await.unwrap();
    writer.write(&ev("order.created", "c")).await.unwrap();
    writer.sync().await.unwrap();

    let reader = FsReader::open(dir.path(), fast()).unwrap();
    let keys = drain(
        &reader,
        FsSubscription {
            filter: EventFilter {
                topic: Some(TopicPattern::exact(Topic::new("order.created").unwrap())),
                ..EventFilter::default()
            },
            stop_at: StopAt::CurrentEnd,
            ..FsSubscription::earliest()
        },
    )
    .await;

    assert_eq!(keys, vec!["a", "c"]);
}

#[tokio::test]
async fn organization_filter_is_applied() {
    let dir = tempfile::tempdir().unwrap();
    let writer = FsWriter::open(dir.path(), FsWriterConfig::default()).unwrap();
    writer
        .write(&org_ev("acme", "order.created", "a"))
        .await
        .unwrap();
    writer
        .write(&org_ev("globex", "order.created", "b"))
        .await
        .unwrap();
    writer.sync().await.unwrap();

    let reader = FsReader::open(dir.path(), fast()).unwrap();
    let keys = drain(
        &reader,
        FsSubscription {
            filter: EventFilter::for_organization(OrganizationId::new("globex").unwrap()),
            stop_at: StopAt::CurrentEnd,
            ..FsSubscription::earliest()
        },
    )
    .await;

    assert_eq!(keys, vec!["b"]);
}

#[tokio::test]
async fn limit_caps_delivery() {
    let dir = tempfile::tempdir().unwrap();
    let writer = FsWriter::open(dir.path(), FsWriterConfig::default()).unwrap();
    for i in 0..10 {
        writer
            .write(&ev("order.created", &format!("k{i}")))
            .await
            .unwrap();
    }
    writer.sync().await.unwrap();

    let reader = FsReader::open(dir.path(), fast()).unwrap();
    let keys = drain(
        &reader,
        FsSubscription {
            limit: Some(4),
            ..FsSubscription::earliest()
        },
    )
    .await;

    assert_eq!(keys.len(), 4);
}

#[tokio::test]
async fn nack_redelivers_the_same_event() {
    let dir = tempfile::tempdir().unwrap();
    let writer = FsWriter::open(dir.path(), FsWriterConfig::default()).unwrap();
    writer.write(&ev("order.created", "only")).await.unwrap();
    writer.sync().await.unwrap();

    let reader = FsReader::open(dir.path(), fast()).unwrap();
    let mut stream = reader.read(FsSubscription::earliest()).await.unwrap();

    let first = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first.event().key().as_str(), "only");
    first.nack().await.unwrap();

    let second = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(second.event().key().as_str(), "only");
    assert_eq!(second.cursor().offset, first.cursor().offset);
}

#[tokio::test]
async fn partition_selection_restricts_delivery() {
    let dir = tempfile::tempdir().unwrap();
    let writer = FsWriter::open(dir.path(), partitioned(4)).unwrap();
    for i in 0..40 {
        writer
            .write(&ev("order.created", &format!("k{i}")))
            .await
            .unwrap();
    }
    writer.sync().await.unwrap();

    let reader = FsReader::open(dir.path(), fast()).unwrap();
    let target = Partition::new(2, NonZeroU32::new(4).unwrap()).unwrap();
    let read = collect(
        &reader,
        FsSubscription {
            partitions: PartitionSelection::Many(PartitionGroup::singleton(target)),
            stop_at: StopAt::CurrentEnd,
            ..FsSubscription::earliest()
        },
        40,
    )
    .await;

    assert!(!read.is_empty());
    assert!(read.iter().all(|(_, c)| c.partition.id() == 2));
}

#[tokio::test]
async fn every_event_is_delivered_exactly_once_across_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let writer = FsWriter::open(dir.path(), partitioned(4)).unwrap();
    for i in 0..64 {
        writer
            .write(&ev("order.created", &format!("k{i}")))
            .await
            .unwrap();
    }
    writer.sync().await.unwrap();

    let reader = FsReader::open(dir.path(), fast()).unwrap();
    let mut keys = drain(
        &reader,
        FsSubscription {
            stop_at: StopAt::CurrentEnd,
            ..FsSubscription::earliest()
        },
    )
    .await;
    keys.sort();
    keys.dedup();

    assert_eq!(keys.len(), 64);
}

#[tokio::test]
async fn reader_tails_events_appended_after_subscribing() {
    let dir = tempfile::tempdir().unwrap();
    let writer = FsWriter::open(dir.path(), FsWriterConfig::default()).unwrap();
    let reader = FsReader::open(dir.path(), fast()).unwrap();
    let mut stream = reader.read(FsSubscription::earliest()).await.unwrap();

    for i in 0..3 {
        writer
            .write(&ev("order.created", &format!("k{i}")))
            .await
            .unwrap();
        writer.sync().await.unwrap();
    }

    let mut seen = Vec::new();
    while seen.len() < 3 {
        let message = timeout(Duration::from_secs(5), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        message.ack().await.unwrap();
        seen.push(message.event().key().as_str().to_owned());
    }

    assert_eq!(seen, vec!["k0", "k1", "k2"]);
}

#[tokio::test]
async fn writer_reopen_continues_the_offset_sequence() {
    let dir = tempfile::tempdir().unwrap();
    {
        let writer = FsWriter::open(dir.path(), FsWriterConfig::default()).unwrap();
        writer.write(&ev("order.created", "a")).await.unwrap();
        writer.sync().await.unwrap();
    }
    let writer = FsWriter::open(dir.path(), FsWriterConfig::default()).unwrap();
    writer.write(&ev("order.created", "b")).await.unwrap();
    writer.sync().await.unwrap();

    assert_eq!(writer.next_offset(0).await.unwrap(), 2);
}

#[tokio::test]
async fn opening_with_a_different_partition_count_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let _writer = FsWriter::open(dir.path(), partitioned(4)).unwrap();
    drop(_writer);

    let result = FsWriter::open(dir.path(), partitioned(8));

    assert!(result.is_err());
}

#[tokio::test]
async fn reader_requires_an_initialised_log() {
    let dir = tempfile::tempdir().unwrap();

    assert!(FsReader::open(dir.path(), fast()).is_err());
}

#[tokio::test]
async fn writers_can_own_disjoint_partition_subsets() {
    let dir = tempfile::tempdir().unwrap();
    let a = FsWriter::open_partitions_subset(dir.path(), partitioned(4), vec![0, 1]).unwrap();
    let b = FsWriter::open_partitions_subset(dir.path(), partitioned(4), vec![2, 3]).unwrap();

    assert_eq!(a.owned_partitions(), vec![0, 1]);
    assert_eq!(b.owned_partitions(), vec![2, 3]);
}

#[tokio::test]
async fn writing_to_an_unowned_partition_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let writer = FsWriter::open_partitions_subset(dir.path(), partitioned(4), vec![0]).unwrap();

    let mut rejected = false;
    for i in 0..40 {
        if writer
            .write(&ev("order.created", &format!("k{i}")))
            .await
            .is_err()
        {
            rejected = true;
            break;
        }
    }

    assert!(rejected);
}
