use std::collections::{BTreeMap, HashSet};
use std::num::NonZeroU32;
use std::path::Path;
use std::time::Duration;

use futures::StreamExt;
use tokio::time::timeout;

use eventuary_core::io::{Reader, Writer};
use eventuary_core::{Event, HasPartition, Payload, StopAt};
use eventuary_fs::log::{LogConfig, SegmentConfig};
use eventuary_fs::reader::{FsReader, FsReaderConfig, FsSubscription};
use eventuary_fs::writer::{FsPartitioningConfig, FsWriter, FsWriterConfig};

const WRITERS: u32 = 4;
const PER_WRITER: u32 = 25;

fn ev(key: &str) -> Event {
    Event::builder(
        "acme",
        "/orders",
        "order.created",
        key,
        Payload::from_string("payload"),
    )
    .unwrap()
    .build()
    .unwrap()
}

fn config(partitions: u32, segment_bytes: u64) -> FsWriterConfig {
    FsWriterConfig {
        log: LogConfig {
            segment: SegmentConfig {
                index_interval_bytes: 64,
                max_bytes: segment_bytes,
            },
            ..LogConfig::default()
        },
        partitioning: FsPartitioningConfig::by_event_key(NonZeroU32::new(partitions).unwrap()),
    }
}

async fn read_back(root: &Path, expected: usize) -> Vec<(u32, u64, String)> {
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
        let Ok(Some(item)) = timeout(Duration::from_secs(10), stream.next()).await else {
            break;
        };
        let message = item.unwrap();
        out.push((
            message.cursor().partition().id(),
            message.cursor().offset(),
            message.event().key().as_str().to_owned(),
        ));
        message.ack().await.unwrap();
    }
    out
}

fn assert_contiguous_per_partition(records: &[(u32, u64, String)], expected: usize) {
    assert_eq!(
        records.len(),
        expected,
        "every event must come back exactly once"
    );

    let mut by_partition: BTreeMap<u32, Vec<u64>> = BTreeMap::new();
    for (partition, offset, _) in records {
        by_partition.entry(*partition).or_default().push(*offset);
    }
    for (partition, mut offsets) in by_partition {
        offsets.sort_unstable();
        let want: Vec<u64> = (0..offsets.len() as u64).collect();
        assert_eq!(
            offsets, want,
            "partition {partition} must be one contiguous run with no hole or reuse"
        );
    }

    let keys: HashSet<&str> = records.iter().map(|(_, _, key)| key.as_str()).collect();
    assert_eq!(
        keys.len(),
        expected,
        "no event may be overwritten by another writer"
    );
}

async fn write_concurrently(
    root: &Path,
    writer_config: impl Fn() -> FsWriterConfig + Send + Sync + 'static,
) {
    let config = std::sync::Arc::new(writer_config);
    let mut tasks = Vec::new();
    for w in 0..WRITERS {
        let root = root.to_path_buf();
        let config = std::sync::Arc::clone(&config);
        tasks.push(tokio::spawn(async move {
            let writer = FsWriter::open(&root, config()).unwrap();
            for i in 0..PER_WRITER {
                writer.write(&ev(&format!("w{w}-{i}"))).await.unwrap();
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
}

#[tokio::test]
async fn concurrent_writers_rolling_segments_lose_no_events() {
    let dir = tempfile::tempdir().unwrap();

    write_concurrently(dir.path(), || config(1, 512)).await;

    let segments = std::fs::read_dir(eventuary_fs::layout::partition_dir(dir.path(), 0))
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|x| x == "log"))
        .count();
    assert!(
        segments > 1,
        "the segment cap must have rolled during the run, or this proves nothing: {segments} segment(s)"
    );

    let records = read_back(dir.path(), (WRITERS * PER_WRITER) as usize).await;
    assert_contiguous_per_partition(&records, (WRITERS * PER_WRITER) as usize);
}

#[tokio::test]
async fn concurrent_writers_across_partitions_keep_each_partition_contiguous() {
    let dir = tempfile::tempdir().unwrap();

    write_concurrently(dir.path(), || config(4, 64 * 1024)).await;

    let records = read_back(dir.path(), (WRITERS * PER_WRITER) as usize).await;
    assert_contiguous_per_partition(&records, (WRITERS * PER_WRITER) as usize);
    let partitions: HashSet<u32> = records.iter().map(|(p, _, _)| *p).collect();
    assert!(
        partitions.len() > 1,
        "the keys must have spread over several partitions"
    );
}

#[tokio::test]
async fn a_reader_tailing_the_log_sees_every_concurrently_written_event() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let expected = (WRITERS * PER_WRITER) as usize;

    FsWriter::open(dir.path(), config(1, 512)).unwrap();

    let tailing = tokio::spawn(async move {
        let reader = FsReader::open(&root, FsReaderConfig::default()).unwrap();
        let mut stream = reader.read(FsSubscription::earliest()).await.unwrap();
        let mut seen = Vec::new();
        while seen.len() < expected {
            let Ok(Some(item)) = timeout(Duration::from_secs(10), stream.next()).await else {
                break;
            };
            let message = item.unwrap();
            seen.push(message.event().key().as_str().to_owned());
            message.ack().await.unwrap();
        }
        seen
    });

    write_concurrently(dir.path(), || config(1, 512)).await;

    let seen = tailing.await.unwrap();
    let unique: HashSet<&str> = seen.iter().map(String::as_str).collect();

    let settled = read_back(dir.path(), expected).await;
    let on_disk: HashSet<&str> = settled.iter().map(|(_, _, key)| key.as_str()).collect();
    let missed: Vec<&&str> = on_disk.difference(&unique).collect();
    assert_eq!(
        on_disk.len(),
        expected,
        "the log itself must hold every event: {} of {expected}",
        on_disk.len()
    );
    assert!(
        missed.is_empty(),
        "the tailing reader missed events the log holds: {missed:?}"
    );
    assert_eq!(
        unique.len(),
        expected,
        "a reader tailing a log under concurrent append must see every event exactly once, saw {} of {expected}",
        unique.len()
    );
}
#[tokio::test]
async fn a_reader_sees_events_appended_to_a_segment_just_before_it_rolled() {
    let dir = tempfile::tempdir().unwrap();
    let writer = FsWriter::open(dir.path(), config(1, 512)).unwrap();

    let reader = FsReader::open(
        dir.path(),
        FsReaderConfig {
            poll_interval: Duration::from_millis(300),
            ..FsReaderConfig::default()
        },
    )
    .unwrap();
    let mut stream = reader.read(FsSubscription::earliest()).await.unwrap();

    writer.write(&ev("before-poll")).await.unwrap();
    let first = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first.event().key().as_str(), "before-poll");
    first.ack().await.unwrap();

    writer.write(&ev("last-of-segment")).await.unwrap();
    for i in 0..8 {
        writer.write(&ev(&format!("after-roll-{i}"))).await.unwrap();
    }

    let mut seen = Vec::new();
    while seen.len() < 9 {
        let Ok(Some(item)) = timeout(Duration::from_secs(5), stream.next()).await else {
            break;
        };
        let message = item.unwrap();
        seen.push(message.event().key().as_str().to_owned());
        message.ack().await.unwrap();
    }

    let segments = std::fs::read_dir(eventuary_fs::layout::partition_dir(dir.path(), 0))
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|x| x == "log"))
        .count();
    assert!(
        segments > 1,
        "the log must have rolled: {segments} segment(s)"
    );
    assert!(
        seen.contains(&"last-of-segment".to_owned()),
        "an event appended to a segment before it rolled must still be delivered, saw {seen:?}"
    );
}

#[tokio::test]
async fn reader_opened_on_an_empty_log_sees_the_first_event() {
    for attempt in 0..20 {
        let dir = tempfile::tempdir().unwrap();
        let writer = FsWriter::open(dir.path(), config(1, 512)).unwrap();

        let reader = FsReader::open(dir.path(), FsReaderConfig::default()).unwrap();
        let mut stream = reader.read(FsSubscription::earliest()).await.unwrap();

        writer.write(&ev("first")).await.unwrap();

        let got = timeout(Duration::from_secs(3), stream.next()).await;
        let message = got
            .unwrap_or_else(|_| panic!("attempt {attempt}: reader never saw the first event"))
            .unwrap()
            .unwrap();
        assert_eq!(message.event().key().as_str(), "first", "attempt {attempt}");
    }
}

#[tokio::test]
async fn a_hole_in_a_partition_is_reported_rather_than_skipped() {
    let dir = tempfile::tempdir().unwrap();
    let writer = FsWriter::open(dir.path(), config(1, 1024)).unwrap();
    for i in 0..6 {
        writer.write(&ev(&format!("e{i}"))).await.unwrap();
    }
    drop(writer);

    let partition = eventuary_fs::layout::partition_dir(dir.path(), 0);
    let first_segment =
        eventuary_fs::layout::segment_path(&partition, 0, eventuary_fs::layout::LOG_SUFFIX);
    let records: Vec<String> = std::fs::read_to_string(&first_segment)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    assert!(
        records.len() > 1,
        "the first segment must hold more than one record for its last to be removable"
    );
    let kept = &records[..records.len() - 1];
    std::fs::write(&first_segment, format!("{}\n", kept.join("\n"))).unwrap();

    let reader = FsReader::open(dir.path(), FsReaderConfig::default()).unwrap();
    let mut stream = reader
        .read(FsSubscription {
            stop_at: StopAt::CurrentEnd,
            ..FsSubscription::earliest()
        })
        .await
        .unwrap();

    let mut error = None;
    while let Ok(Some(item)) = timeout(Duration::from_secs(5), stream.next()).await {
        match item {
            Ok(message) => message.ack().await.unwrap(),
            Err(e) => {
                error = Some(e);
                break;
            }
        }
    }

    let error = error.expect("a missing offset must surface, not be skipped");
    assert!(
        error.to_string().contains("has no event at offset"),
        "unexpected error: {error}"
    );
}
