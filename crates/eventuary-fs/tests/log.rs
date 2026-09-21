use std::fs::OpenOptions;
use std::io::Write;
use std::num::NonZeroU32;
use std::time::Duration;

use eventuary_core::{Event, Payload, SerializedEvent};
use eventuary_fs::layout::{LOG_SUFFIX, partition_dir, segment_path};
use eventuary_fs::log::SegmentConfig;
use eventuary_fs::log::{LogConfig, PartitionLog, RetentionPolicy, SyncPolicy};

fn serialized(key: &str, topic: &str) -> SerializedEvent {
    let event = Event::builder("acme", "/orders", topic, key, Payload::from_string("{}"))
        .unwrap()
        .build()
        .unwrap();
    SerializedEvent::from_event(&event).unwrap()
}

fn small_segments(max_bytes: u64) -> LogConfig {
    LogConfig {
        segment: SegmentConfig {
            index_interval_bytes: 64,
            max_bytes,
        },
        ..LogConfig::default()
    }
}

#[test]
fn empty_log_starts_at_zero() {
    let dir = tempfile::tempdir().unwrap();
    let log = PartitionLog::open_writable(dir.path(), 0, LogConfig::default()).unwrap();

    assert_eq!(log.next_offset(), 0);
    assert_eq!(log.start_offset(), 0);
    assert!(log.is_empty());
}

#[test]
fn append_assigns_monotonic_offsets_from_zero() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = PartitionLog::open_writable(dir.path(), 0, LogConfig::default()).unwrap();

    let offsets: Vec<u64> = (0..5)
        .map(|i| {
            log.append(serialized(&format!("k{i}"), "order.created"))
                .unwrap()
        })
        .collect();

    assert_eq!(offsets, vec![0, 1, 2, 3, 4]);
    assert_eq!(log.next_offset(), 5);
}

#[test]
fn read_returns_records_in_offset_order() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = PartitionLog::open_writable(dir.path(), 0, LogConfig::default()).unwrap();
    for i in 0..10 {
        log.append(serialized(&format!("k{i}"), "order.created"))
            .unwrap();
    }

    let records = log.read(0, 100).unwrap();

    assert_eq!(records.len(), 10);
    assert_eq!(
        records.iter().map(|r| r.offset).collect::<Vec<_>>(),
        (0..10).collect::<Vec<_>>()
    );
}

#[test]
fn read_from_offset_skips_earlier_records() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = PartitionLog::open_writable(dir.path(), 0, LogConfig::default()).unwrap();
    for i in 0..10 {
        log.append(serialized(&format!("k{i}"), "order.created"))
            .unwrap();
    }

    let records = log.read(7, 100).unwrap();

    assert_eq!(records.first().map(|r| r.offset), Some(7));
    assert_eq!(records.len(), 3);
}

#[test]
fn read_honours_max_records() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = PartitionLog::open_writable(dir.path(), 0, LogConfig::default()).unwrap();
    for i in 0..50 {
        log.append(serialized(&format!("k{i}"), "order.created"))
            .unwrap();
    }

    let records = log.read(0, 8).unwrap();

    assert_eq!(records.len(), 8);
}

#[test]
fn read_past_end_returns_empty() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = PartitionLog::open_writable(dir.path(), 0, LogConfig::default()).unwrap();
    log.append(serialized("k", "order.created")).unwrap();

    assert!(log.read(99, 10).unwrap().is_empty());
}

#[test]
fn segments_roll_when_max_bytes_exceeded() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = PartitionLog::open_writable(dir.path(), 0, small_segments(512)).unwrap();
    for i in 0..40 {
        log.append(serialized(&format!("k{i}"), "order.created"))
            .unwrap();
    }

    assert!(log.segment_count() > 1);
    assert_eq!(log.read(0, 1000).unwrap().len(), 40);
}

#[test]
fn reads_span_segment_boundaries_contiguously() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = PartitionLog::open_writable(dir.path(), 0, small_segments(400)).unwrap();
    for i in 0..60 {
        log.append(serialized(&format!("k{i}"), "order.created"))
            .unwrap();
    }

    let records = log.read(0, 1000).unwrap();

    assert!(log.segment_count() > 2);
    assert_eq!(
        records.iter().map(|r| r.offset).collect::<Vec<_>>(),
        (0..60).collect::<Vec<_>>()
    );
}

#[test]
fn reopen_resumes_offsets_after_existing_records() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut log = PartitionLog::open_writable(dir.path(), 0, LogConfig::default()).unwrap();
        for i in 0..7 {
            log.append(serialized(&format!("k{i}"), "order.created"))
                .unwrap();
        }
        log.sync().unwrap();
    }

    let mut log = PartitionLog::open_writable(dir.path(), 0, LogConfig::default()).unwrap();

    assert_eq!(log.next_offset(), 7);
    assert_eq!(log.append(serialized("k7", "order.created")).unwrap(), 7);
    assert_eq!(log.read(0, 100).unwrap().len(), 8);
}

#[test]
fn reopen_recovers_after_torn_trailing_write() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut log = PartitionLog::open_writable(dir.path(), 0, LogConfig::default()).unwrap();
        for i in 0..5 {
            log.append(serialized(&format!("k{i}"), "order.created"))
                .unwrap();
        }
        log.sync().unwrap();
    }
    let segment = segment_path(&partition_dir(dir.path(), 0), 0, LOG_SUFFIX);
    let mut file = OpenOptions::new().append(true).open(&segment).unwrap();
    file.write_all(br#"{"offset":5,"id":"truncated"#).unwrap();
    file.sync_all().unwrap();

    let mut log = PartitionLog::open_writable(dir.path(), 0, LogConfig::default()).unwrap();

    assert_eq!(log.next_offset(), 5);
    assert_eq!(log.read(0, 100).unwrap().len(), 5);
    assert_eq!(log.append(serialized("k5", "order.created")).unwrap(), 5);
    assert_eq!(log.read(0, 100).unwrap().len(), 6);
}

#[test]
fn second_writer_on_same_partition_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let _first = PartitionLog::open_writable(dir.path(), 0, LogConfig::default()).unwrap();

    let second = PartitionLog::open_writable(dir.path(), 0, LogConfig::default());

    assert!(second.is_err());
}

#[test]
fn readers_are_unaffected_by_the_writer_lock() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = PartitionLog::open_writable(dir.path(), 0, LogConfig::default()).unwrap();
    writer.append(serialized("k", "order.created")).unwrap();
    writer.sync().unwrap();

    let reader = PartitionLog::open_readonly(dir.path(), 0, LogConfig::default()).unwrap();

    assert_eq!(reader.read(0, 10).unwrap().len(), 1);
}

#[test]
fn lock_is_released_when_the_writer_is_dropped() {
    let dir = tempfile::tempdir().unwrap();
    {
        let _first = PartitionLog::open_writable(dir.path(), 0, LogConfig::default()).unwrap();
    }

    assert!(PartitionLog::open_writable(dir.path(), 0, LogConfig::default()).is_ok());
}

#[test]
fn different_partitions_lock_independently() {
    let dir = tempfile::tempdir().unwrap();
    let _p0 = PartitionLog::open_writable(dir.path(), 0, LogConfig::default()).unwrap();

    assert!(PartitionLog::open_writable(dir.path(), 1, LogConfig::default()).is_ok());
    assert!(NonZeroU32::new(2).is_some());
}

#[test]
fn offset_for_timestamp_finds_first_record_at_or_after() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = PartitionLog::open_writable(dir.path(), 0, small_segments(400)).unwrap();
    let mut timestamps = Vec::new();
    for i in 0..30 {
        let event = serialized(&format!("k{i}"), "order.created");
        timestamps.push(event.timestamp);
        log.append(event).unwrap();
    }

    let target = timestamps[20].timestamp_millis();
    let offset = log.offset_for_timestamp(target).unwrap();

    let record = log.read(offset, 1).unwrap().remove(0);
    assert!(record.event.timestamp.timestamp_millis() >= target);
    assert!(offset <= 20);
}

#[test]
fn retention_by_size_drops_oldest_segments_and_keeps_the_active_one() {
    let dir = tempfile::tempdir().unwrap();
    let config = LogConfig {
        segment: SegmentConfig {
            index_interval_bytes: 64,
            max_bytes: 400,
        },
        sync: SyncPolicy::Always,
        retention: RetentionPolicy {
            max_bytes: Some(800),
            max_age: None,
        },
        lock_wait: std::time::Duration::ZERO,
    };
    let mut log = PartitionLog::open_writable(dir.path(), 0, config).unwrap();
    for i in 0..80 {
        log.append(serialized(&format!("k{i}"), "order.created"))
            .unwrap();
    }
    let before = log.segment_count();

    let removed = log.enforce_retention().unwrap();

    assert!(removed > 0);
    assert!(log.segment_count() < before);
    assert!(log.segment_count() >= 1);
    assert!(log.start_offset() > 0);
}

#[test]
fn retention_never_removes_the_only_segment() {
    let dir = tempfile::tempdir().unwrap();
    let config = LogConfig {
        retention: RetentionPolicy {
            max_bytes: Some(1),
            max_age: Some(Duration::from_secs(0)),
        },
        ..LogConfig::default()
    };
    let mut log = PartitionLog::open_writable(dir.path(), 0, config).unwrap();
    log.append(serialized("k", "order.created")).unwrap();

    assert_eq!(log.enforce_retention().unwrap(), 0);
    assert_eq!(log.segment_count(), 1);
}

#[test]
fn sync_always_persists_each_append() {
    let dir = tempfile::tempdir().unwrap();
    let config = LogConfig {
        sync: SyncPolicy::Always,
        ..LogConfig::default()
    };
    let mut log = PartitionLog::open_writable(dir.path(), 0, config).unwrap();
    log.append(serialized("k", "order.created")).unwrap();

    let reader = PartitionLog::open_readonly(dir.path(), 0, LogConfig::default()).unwrap();

    assert_eq!(reader.read(0, 10).unwrap().len(), 1);
}

#[test]
fn append_all_returns_contiguous_offsets() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = PartitionLog::open_writable(dir.path(), 0, LogConfig::default()).unwrap();

    let offsets = log
        .append_all(
            (0..4)
                .map(|i| serialized(&format!("k{i}"), "order.created"))
                .collect(),
        )
        .unwrap();

    assert_eq!(offsets, vec![0, 1, 2, 3]);
}

#[test]
fn sparse_index_grows_with_the_configured_interval() {
    let dir = tempfile::tempdir().unwrap();
    let config = LogConfig {
        segment: SegmentConfig {
            index_interval_bytes: 4096,
            max_bytes: 1024 * 1024,
        },
        ..LogConfig::default()
    };
    let mut log = PartitionLog::open_writable(dir.path(), 0, config).unwrap();
    for i in 0..100 {
        log.append(serialized(&format!("k{i}"), "order.created"))
            .unwrap();
    }
    log.sync().unwrap();

    let (offset_entries, time_entries) =
        eventuary_fs::log::index_entry_counts(&partition_dir(dir.path(), 0), 0).unwrap();

    assert!(offset_entries > 1);
    assert!(offset_entries < 100);
    assert_eq!(offset_entries, time_entries);
}

#[test]
fn seeking_a_high_offset_does_not_rescan_from_the_start() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = PartitionLog::open_writable(dir.path(), 0, small_segments(1024 * 1024)).unwrap();
    for i in 0..500 {
        log.append(serialized(&format!("k{i}"), "order.created"))
            .unwrap();
    }

    let records = log.read(499, 10).unwrap();

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].offset, 499);
}

#[test]
fn the_default_sync_policy_fsyncs_periodically() {
    assert_eq!(
        LogConfig::default().sync,
        SyncPolicy::EveryBytes(eventuary_fs::log::DEFAULT_SYNC_INTERVAL_BYTES)
    );
    assert_ne!(LogConfig::default().sync, SyncPolicy::Never);
}
