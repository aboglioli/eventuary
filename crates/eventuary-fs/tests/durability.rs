use std::fs::{self, OpenOptions};
use std::io::Write;

use eventuary_core::{Event, Payload, SerializedEvent};
use eventuary_fs::layout::{
    LOG_SUFFIX, OFFSET_INDEX_SUFFIX, TIME_INDEX_SUFFIX, partition_dir, segment_path,
};
use eventuary_fs::log::SegmentConfig;
use eventuary_fs::log::{LogConfig, PartitionLog, SyncPolicy};

fn serialized(key: &str) -> SerializedEvent {
    let event = Event::builder(
        "acme",
        "/orders",
        "order.created",
        key,
        Payload::from_string("payload"),
    )
    .unwrap()
    .build()
    .unwrap();
    SerializedEvent::from_event(&event).unwrap()
}

fn synced() -> LogConfig {
    LogConfig {
        sync: SyncPolicy::Always,
        ..LogConfig::default()
    }
}

fn seed(root: &std::path::Path, count: usize, config: LogConfig) {
    let mut log = PartitionLog::open_writable(root, 0, config).unwrap();
    for i in 0..count {
        log.append(serialized(&format!("k{i}"))).unwrap();
    }
    log.sync().unwrap();
}

#[test]
fn torn_trailing_write_is_discarded_and_its_offset_is_reused() {
    let dir = tempfile::tempdir().unwrap();
    seed(dir.path(), 4, synced());
    let segment = segment_path(&partition_dir(dir.path(), 0), 0, LOG_SUFFIX);
    let mut file = OpenOptions::new().append(true).open(&segment).unwrap();
    file.write_all(br#"{"offset":4,"id":"7f8"#).unwrap();
    file.sync_all().unwrap();

    let mut log = PartitionLog::open_writable(dir.path(), 0, synced()).unwrap();

    assert_eq!(log.next_offset(), 4);
    assert_eq!(log.append(serialized("replacement")).unwrap(), 4);
    let records = log.read(0, 100).unwrap();
    assert_eq!(records.len(), 5);
    assert_eq!(records[4].event.key, "replacement");
}

#[test]
fn trailing_garbage_without_a_newline_is_discarded() {
    let dir = tempfile::tempdir().unwrap();
    seed(dir.path(), 3, synced());
    let segment = segment_path(&partition_dir(dir.path(), 0), 0, LOG_SUFFIX);
    let mut file = OpenOptions::new().append(true).open(&segment).unwrap();
    file.write_all(b"\x00\x00\x00garbage").unwrap();
    file.sync_all().unwrap();

    let log = PartitionLog::open_writable(dir.path(), 0, synced()).unwrap();

    assert_eq!(log.next_offset(), 3);
    assert_eq!(log.read(0, 100).unwrap().len(), 3);
}

#[test]
fn a_complete_but_unparseable_line_stops_recovery_there() {
    let dir = tempfile::tempdir().unwrap();
    seed(dir.path(), 3, synced());
    let segment = segment_path(&partition_dir(dir.path(), 0), 0, LOG_SUFFIX);
    let mut file = OpenOptions::new().append(true).open(&segment).unwrap();
    file.write_all(b"{\"not\":\"a record\"}\n").unwrap();
    file.sync_all().unwrap();

    let mut log = PartitionLog::open_writable(dir.path(), 0, synced()).unwrap();

    assert_eq!(log.next_offset(), 3);
    assert_eq!(log.append(serialized("after")).unwrap(), 3);
    assert_eq!(log.read(0, 100).unwrap().len(), 4);
}

#[test]
fn a_deleted_offset_index_is_tolerated_and_reads_still_work() {
    let dir = tempfile::tempdir().unwrap();
    seed(dir.path(), 50, synced());
    fs::remove_file(segment_path(
        &partition_dir(dir.path(), 0),
        0,
        OFFSET_INDEX_SUFFIX,
    ))
    .unwrap();

    let log = PartitionLog::open_readonly(dir.path(), 0, synced()).unwrap();

    assert_eq!(log.read(0, 100).unwrap().len(), 50);
    assert_eq!(log.read(40, 100).unwrap().len(), 10);
}

#[test]
fn a_truncated_offset_index_is_tolerated() {
    let dir = tempfile::tempdir().unwrap();
    seed(
        dir.path(),
        60,
        LogConfig {
            segment: SegmentConfig {
                index_interval_bytes: 128,
                max_bytes: 1024 * 1024,
            },
            sync: SyncPolicy::Always,
            ..LogConfig::default()
        },
    );
    let index = segment_path(&partition_dir(dir.path(), 0), 0, OFFSET_INDEX_SUFFIX);
    let len = fs::metadata(&index).unwrap().len();
    OpenOptions::new()
        .write(true)
        .open(&index)
        .unwrap()
        .set_len(len / 2 + 3)
        .unwrap();

    let log = PartitionLog::open_readonly(dir.path(), 0, synced()).unwrap();

    assert_eq!(log.read(0, 200).unwrap().len(), 60);
    assert_eq!(log.read(55, 200).unwrap().first().unwrap().offset, 55);
}

#[test]
fn a_deleted_time_index_is_tolerated() {
    let dir = tempfile::tempdir().unwrap();
    seed(dir.path(), 20, synced());
    fs::remove_file(segment_path(
        &partition_dir(dir.path(), 0),
        0,
        TIME_INDEX_SUFFIX,
    ))
    .unwrap();

    let log = PartitionLog::open_readonly(dir.path(), 0, synced()).unwrap();

    assert_eq!(log.read(0, 100).unwrap().len(), 20);
}

#[test]
fn a_record_larger_than_the_segment_limit_is_still_written() {
    let dir = tempfile::tempdir().unwrap();
    let config = LogConfig {
        segment: SegmentConfig {
            index_interval_bytes: 64,
            max_bytes: 64,
        },
        sync: SyncPolicy::Always,
        ..LogConfig::default()
    };
    let mut log = PartitionLog::open_writable(dir.path(), 0, config).unwrap();

    let event = Event::builder(
        "acme",
        "/orders",
        "order.created",
        "big",
        Payload::from_string("x".repeat(4096)),
    )
    .unwrap()
    .build()
    .unwrap();
    let offset = log
        .append(SerializedEvent::from_event(&event).unwrap())
        .unwrap();

    assert_eq!(offset, 0);
    assert_eq!(log.read(0, 10).unwrap().len(), 1);
}

#[test]
fn an_empty_segment_file_does_not_break_open() {
    let dir = tempfile::tempdir().unwrap();
    seed(dir.path(), 2, synced());
    let stray = segment_path(&partition_dir(dir.path(), 0), 999, LOG_SUFFIX);
    fs::write(&stray, b"").unwrap();

    let log = PartitionLog::open_readonly(dir.path(), 0, synced()).unwrap();

    assert_eq!(log.read(0, 10).unwrap().len(), 2);
}

#[test]
fn unrelated_files_in_a_partition_directory_are_ignored() {
    let dir = tempfile::tempdir().unwrap();
    seed(dir.path(), 3, synced());
    fs::write(partition_dir(dir.path(), 0).join("notes.txt"), b"hello").unwrap();
    fs::write(partition_dir(dir.path(), 0).join("123.log"), b"junk\n").unwrap();

    let log = PartitionLog::open_readonly(dir.path(), 0, synced()).unwrap();

    assert_eq!(log.read(0, 10).unwrap().len(), 3);
}

#[test]
fn reads_after_a_roll_see_every_record_across_segments() {
    let dir = tempfile::tempdir().unwrap();
    let config = LogConfig {
        segment: SegmentConfig {
            index_interval_bytes: 128,
            max_bytes: 600,
        },
        sync: SyncPolicy::Always,
        ..LogConfig::default()
    };
    seed(dir.path(), 100, config);

    let log = PartitionLog::open_readonly(dir.path(), 0, config).unwrap();
    let all = log.read(0, 1000).unwrap();

    assert!(log.segment_count() > 3);
    assert_eq!(all.len(), 100);
    assert_eq!(
        all.iter().map(|r| r.offset).collect::<Vec<_>>(),
        (0..100).collect::<Vec<_>>()
    );
}

#[test]
fn every_offset_is_individually_addressable_after_rolls() {
    let dir = tempfile::tempdir().unwrap();
    let config = LogConfig {
        segment: SegmentConfig {
            index_interval_bytes: 128,
            max_bytes: 600,
        },
        sync: SyncPolicy::Always,
        ..LogConfig::default()
    };
    seed(dir.path(), 60, config);
    let log = PartitionLog::open_readonly(dir.path(), 0, config).unwrap();

    for offset in 0..60u64 {
        let record = log.read(offset, 1).unwrap();
        assert_eq!(record.first().map(|r| r.offset), Some(offset));
    }
}

#[test]
fn refresh_exposes_records_appended_by_another_handle() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = PartitionLog::open_writable(dir.path(), 0, synced()).unwrap();
    writer.append(serialized("first")).unwrap();
    let mut reader = PartitionLog::open_readonly(dir.path(), 0, synced()).unwrap();
    assert_eq!(reader.read(0, 10).unwrap().len(), 1);

    writer.append(serialized("second")).unwrap();
    reader.refresh().unwrap();

    assert_eq!(reader.read(0, 10).unwrap().len(), 2);
}

#[test]
fn refresh_picks_up_segments_created_after_open() {
    let dir = tempfile::tempdir().unwrap();
    let config = LogConfig {
        segment: SegmentConfig {
            index_interval_bytes: 128,
            max_bytes: 400,
        },
        sync: SyncPolicy::Always,
        ..LogConfig::default()
    };
    let mut writer = PartitionLog::open_writable(dir.path(), 0, config).unwrap();
    writer.append(serialized("k0")).unwrap();
    let mut reader = PartitionLog::open_readonly(dir.path(), 0, config).unwrap();
    let before = reader.segment_count();

    for i in 1..40 {
        writer.append(serialized(&format!("k{i}"))).unwrap();
    }
    reader.refresh().unwrap();

    assert!(reader.segment_count() > before);
    assert_eq!(reader.read(0, 100).unwrap().len(), 40);
}

#[test]
fn a_high_volume_append_and_full_read_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let config = LogConfig {
        segment: SegmentConfig {
            index_interval_bytes: 4096,
            max_bytes: 256 * 1024,
        },
        ..LogConfig::default()
    };
    let mut log = PartitionLog::open_writable(dir.path(), 0, config).unwrap();
    for i in 0..5_000 {
        log.append(serialized(&format!("k{i}"))).unwrap();
    }
    log.sync().unwrap();

    let records = log.read(0, 10_000).unwrap();

    assert_eq!(records.len(), 5_000);
    assert_eq!(records.last().unwrap().offset, 4_999);
    assert!(log.segment_count() > 1);
}

#[test]
fn duplicate_offsets_from_a_foreign_writer_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    seed(dir.path(), 5, synced());
    let segment = segment_path(&partition_dir(dir.path(), 0), 0, LOG_SUFFIX);
    let existing = fs::read_to_string(&segment).unwrap();
    let replayed = existing.lines().last().unwrap().to_owned();
    let mut file = OpenOptions::new().append(true).open(&segment).unwrap();
    file.write_all(format!("{replayed}\n").as_bytes()).unwrap();
    file.sync_all().unwrap();

    let result = PartitionLog::open_readonly(dir.path(), 0, synced());

    assert!(result.is_err(), "a repeated offset must not open cleanly");
}

#[test]
fn a_gap_in_offsets_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    seed(dir.path(), 3, synced());
    let segment = segment_path(&partition_dir(dir.path(), 0), 0, LOG_SUFFIX);
    let mut record: serde_json::Value = serde_json::from_str(
        fs::read_to_string(&segment)
            .unwrap()
            .lines()
            .last()
            .unwrap(),
    )
    .unwrap();
    record["offset"] = serde_json::json!(99);
    let mut file = OpenOptions::new().append(true).open(&segment).unwrap();
    file.write_all(format!("{record}\n").as_bytes()).unwrap();
    file.sync_all().unwrap();

    let result = PartitionLog::open_readonly(dir.path(), 0, synced());

    assert!(result.is_err(), "a gap in offsets must not open cleanly");
}

#[test]
fn a_segment_whose_first_offset_is_not_its_base_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    seed(dir.path(), 2, synced());
    let stray = segment_path(&partition_dir(dir.path(), 0), 500, LOG_SUFFIX);
    let mut record: serde_json::Value = serde_json::from_str(
        fs::read_to_string(segment_path(&partition_dir(dir.path(), 0), 0, LOG_SUFFIX))
            .unwrap()
            .lines()
            .next()
            .unwrap(),
    )
    .unwrap();
    record["offset"] = serde_json::json!(777);
    fs::write(&stray, format!("{record}\n")).unwrap();

    let result = PartitionLog::open_readonly(dir.path(), 0, synced());

    assert!(result.is_err(), "a segment must start at its base offset");
}
