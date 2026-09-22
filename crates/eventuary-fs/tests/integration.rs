use std::num::NonZeroU32;
use std::time::Duration;

use futures::StreamExt;
use tokio::time::timeout;

use eventuary_core::io::filter::{EventFilter, TopicPattern};
use eventuary_core::io::{Reader, Writer};
use eventuary_core::partition::{
    EventKeyPartitionKeyResolver, Fnv1a64PartitionHasher, Partition, PartitionGroup,
    PartitionHasher, PartitionKeyResolver, PartitionSelection,
};
use eventuary_core::{Error, Event, OrganizationId, Payload, StartFrom, StopAt, Topic};
use eventuary_fs::log::{LogConfig, RetentionPolicy, SegmentConfig, WriterAccess};
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

/// Reads one partition back as `(offset, key)` pairs, so a test can assert that concurrent
/// writers left one contiguous offset sequence and lost nothing.
async fn offsets_and_keys(root: &std::path::Path, expected: usize) -> Vec<(u64, String)> {
    let reader = FsReader::open(root, FsReaderConfig::default()).unwrap();
    let mut stream = reader
        .read(FsSubscription {
            stop_at: StopAt::CurrentEnd,
            ..FsSubscription::earliest()
        })
        .await
        .unwrap();
    let mut out = Vec::new();
    while out.len() < expected {
        let Ok(Some(item)) = timeout(Duration::from_secs(5), stream.next()).await else {
            break;
        };
        let message = item.unwrap();
        out.push((
            message.cursor().offset(),
            message.event().key().as_str().to_owned(),
        ));
        message.ack().await.unwrap();
    }
    out
}

fn exclusive(partitions: u32) -> FsWriterConfig {
    FsWriterConfig {
        log: LogConfig {
            access: WriterAccess::Exclusive,
            ..LogConfig::default()
        },
        partitioning: FsPartitioningConfig::by_event_key(NonZeroU32::new(partitions).unwrap()),
    }
}

fn shared(partitions: u32) -> FsWriterConfig {
    FsWriterConfig {
        partitioning: FsPartitioningConfig::by_event_key(NonZeroU32::new(partitions).unwrap()),
        ..FsWriterConfig::default()
    }
}

#[tokio::test]
async fn a_batch_naming_an_unowned_partition_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let writer = FsWriter::open_partitions_subset(dir.path(), shared(4), vec![0]).unwrap();

    let mut batch = Vec::new();
    for i in 0..40 {
        batch.push(ev("t", &format!("k{i}")));
    }
    let refused = writer
        .write_all(&batch)
        .await
        .expect_err("partition 0 is the only one this writer owns");
    assert!(matches!(refused, Error::Config(_)), "{refused:?}");

    assert_eq!(
        writer.next_offset(0).await.unwrap(),
        0,
        "a rejected batch must not have appended part of itself"
    );
}

#[tokio::test]
async fn shared_writers_append_to_one_partition_without_losing_events() {
    let dir = tempfile::tempdir().unwrap();
    let first = FsWriter::open(dir.path(), shared(1)).unwrap();
    let second = FsWriter::open(dir.path(), shared(1)).unwrap();

    for i in 0..25 {
        first.write(&ev("t", &format!("a{i}"))).await.unwrap();
        second.write(&ev("t", &format!("b{i}"))).await.unwrap();
    }

    assert_eq!(first.next_offset(0).await.unwrap(), 50);

    let records = offsets_and_keys(dir.path(), 50).await;
    let offsets: Vec<u64> = records.iter().map(|(offset, _)| *offset).collect();
    assert_eq!(
        offsets,
        (0..50).collect::<Vec<u64>>(),
        "neither writer may reuse an offset or leave a hole"
    );
}

#[tokio::test]
async fn shared_writers_interleave_concurrently_on_one_partition() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();

    let mut writers = Vec::new();
    for w in 0..4 {
        let root = root.clone();
        writers.push(tokio::spawn(async move {
            let writer = FsWriter::open(&root, shared(1)).unwrap();
            for i in 0..25 {
                writer.write(&ev("t", &format!("w{w}-{i}"))).await.unwrap();
            }
        }));
    }
    for task in writers {
        task.await.unwrap();
    }

    let records = offsets_and_keys(dir.path(), 100).await;
    let offsets: Vec<u64> = records.iter().map(|(offset, _)| *offset).collect();
    assert_eq!(offsets, (0..100).collect::<Vec<u64>>());

    let mut keys: Vec<String> = records.iter().map(|(_, key)| key.clone()).collect();
    keys.sort();
    keys.dedup();
    assert_eq!(keys.len(), 100, "every event survives, none overwritten");
}

#[tokio::test]
async fn a_shared_writer_does_not_have_to_be_dropped_to_release_a_partition() {
    let dir = tempfile::tempdir().unwrap();
    let holder = FsWriter::open(dir.path(), shared(1)).unwrap();
    holder.write(&ev("t", "k")).await.unwrap();

    let other = FsWriter::open(dir.path(), shared(1)).unwrap();
    other
        .write(&ev("t", "k"))
        .await
        .expect("a shared writer releases its partition after each write");
}

#[tokio::test]
async fn an_exclusive_writer_keeps_its_partition_until_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let held = FsWriter::open(dir.path(), exclusive(1)).unwrap();
    held.write(&ev("t", "k")).await.unwrap();

    let second = FsWriter::open(dir.path(), exclusive(1)).expect("opening takes no locks");

    let refused = second
        .write(&ev("t", "k"))
        .await
        .expect_err("an exclusive partition has one writer");
    assert!(
        matches!(refused, Error::Contended(_)),
        "contention is reported as such, so a caller can retry it: {refused:?}"
    );
}

#[tokio::test]
async fn exclusive_writers_touching_different_partitions_do_not_meet() {
    let dir = tempfile::tempdir().unwrap();
    let first = FsWriter::open(dir.path(), exclusive(8)).unwrap();
    let second = FsWriter::open(dir.path(), exclusive(8)).unwrap();

    let (ka, kb) = keys_on_different_partitions(8);

    first.write(&ev("t", &ka)).await.unwrap();
    second
        .write(&ev("t", &kb))
        .await
        .expect("disjoint partitions are the whole point of partitioning");
}

fn keys_on_different_partitions(count: u32) -> (String, String) {
    let config = FsPartitioningConfig::by_event_key(NonZeroU32::new(count).unwrap());
    for a in 0..40 {
        for b in (a + 1)..40 {
            let (ka, kb) = (format!("k{a}"), format!("k{b}"));
            if partition_of(&config, &ka) != partition_of(&config, &kb) {
                return (ka, kb);
            }
        }
    }
    panic!("{count} partitions must separate some pair of keys");
}

fn partition_of(config: &FsPartitioningConfig, key: &str) -> u32 {
    Fnv1a64PartitionHasher
        .partition_for(
            &EventKeyPartitionKeyResolver::new()
                .partition_key(&ev("t", key))
                .unwrap(),
            config.partition_count(),
        )
        .id()
}

#[tokio::test]
async fn a_lock_wait_lets_a_contending_writer_take_its_turn() {
    let dir = tempfile::tempdir().unwrap();
    let patient = || FsWriterConfig {
        log: LogConfig {
            lock_wait: Some(Duration::from_secs(2)),
            ..exclusive(1).log
        },
        ..exclusive(1)
    };

    let first = FsWriter::open(dir.path(), patient()).unwrap();
    first.write(&ev("t", "k")).await.unwrap();

    let second = FsWriter::open(dir.path(), patient()).unwrap();

    let releasing = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(60)).await;
        drop(first);
    });

    second
        .write(&ev("t", "k"))
        .await
        .expect("a bounded wait should outlast a writer that is about to finish");
    releasing.await.unwrap();
}

#[tokio::test]
async fn a_lock_wait_gives_up_rather_than_hanging() {
    let dir = tempfile::tempdir().unwrap();
    let _held = FsWriter::open(dir.path(), exclusive(1)).unwrap();
    _held.write(&ev("t", "k")).await.unwrap();

    let impatient = FsWriter::open(
        dir.path(),
        FsWriterConfig {
            log: LogConfig {
                access: WriterAccess::Exclusive,
                lock_wait: Some(Duration::from_millis(100)),
                ..LogConfig::default()
            },
            ..exclusive(1)
        },
    )
    .unwrap();

    let started = std::time::Instant::now();
    let refused = impatient.write(&ev("t", "k")).await.unwrap_err();
    assert!(matches!(refused, Error::Contended(_)));
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "the wait must be bounded, not indefinite"
    );
}

#[tokio::test]
async fn asking_for_the_next_offset_does_not_claim_the_partition() {
    let dir = tempfile::tempdir().unwrap();
    let observer = FsWriter::open(dir.path(), exclusive(1)).unwrap();
    assert_eq!(observer.next_offset(0).await.unwrap(), 0);

    let writer = FsWriter::open(dir.path(), exclusive(1)).unwrap();
    writer
        .write(&ev("t", "k"))
        .await
        .expect("observing a partition must not take its lock");
    assert_eq!(observer.next_offset(0).await.unwrap(), 1);
}

#[tokio::test]
async fn retention_removing_unread_events_surfaces_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let config = FsWriterConfig {
        log: LogConfig {
            segment: SegmentConfig {
                index_interval_bytes: 64,
                max_bytes: 400,
            },
            retention: RetentionPolicy {
                max_bytes: Some(800),
                max_age: None,
            },
            ..LogConfig::default()
        },
        ..FsWriterConfig::default()
    };
    let writer = FsWriter::open(dir.path(), config).unwrap();
    for i in 0..80 {
        writer
            .write(&ev("order.created", &format!("k{i}")))
            .await
            .unwrap();
    }
    writer.sync().await.unwrap();
    assert!(writer.enforce_retention().await.unwrap() > 0);

    let reader = FsReader::open(dir.path(), fast()).unwrap();
    let partition = Partition::new(0, NonZeroU32::new(1).unwrap()).unwrap();
    let subscription = FsSubscription {
        start: StartFrom::After(FsCursor::new(partition, 0)),
        ..FsSubscription::default()
    };
    let mut stream = reader.read(subscription).await.unwrap();

    let first = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("stream must yield")
        .expect("stream must not end");

    match first {
        Err(Error::InvalidCursor(detail)) => {
            assert!(detail.contains("retention removed"), "{detail}");
        }
        Err(other) => panic!("expected a cursor error, got {other}"),
        Ok(message) => panic!(
            "expected a retention gap, got event {}",
            message.event().key().as_str()
        ),
    }
}
