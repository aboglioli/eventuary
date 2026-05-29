use std::num::NonZeroU32;

use eventuary_core::io::Writer;
use eventuary_core::partition::{
    EventKeyPartitionKeyResolver, Fnv1a64PartitionHasher, PartitionHasher, PartitionKey,
};
use eventuary_core::{Error, Event, Payload};
use eventuary_sqlite::database::SqliteDatabase;
use eventuary_sqlite::partitioning::{
    BackfillReport, SqlitePartitionBackfill, SqlitePartitionBackfillConfig,
};
use eventuary_sqlite::relation::SqliteRelationName;
use eventuary_sqlite::writer::{SqlitePartitioningConfig, SqliteWriter, SqliteWriterConfig};

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
async fn sqlite_partition_backfill_populates_partition_columns() {
    let db = SqliteDatabase::open_in_memory().unwrap();
    SqliteWriter::prepare_schema(&db.conn(), &SqliteWriterConfig::default()).unwrap();
    let writer = SqliteWriter::new(db.conn());

    for i in 0..100 {
        writer
            .write(&event_with_key(&format!("k{i}")))
            .await
            .unwrap();
    }

    let partition_count = NonZeroU32::new(4).unwrap();
    let config = SqlitePartitionBackfillConfig::new(
        SqliteRelationName::new("events").unwrap(),
        partition_count,
        EventKeyPartitionKeyResolver::new(),
        Fnv1a64PartitionHasher,
        16,
    )
    .unwrap();
    let backfill = SqlitePartitionBackfill::new(db.conn(), config);
    let report: BackfillReport = backfill.run().await.unwrap();

    assert_eq!(report.rows_updated, 100);
    assert!(
        report.batches >= 7,
        "expected >=7 batches for 100 rows / batch 16, got {}",
        report.batches
    );

    let hasher = Fnv1a64PartitionHasher;
    let conn = db.conn();
    let guard = conn.lock().unwrap();
    type PartitionRow = (
        String,
        Option<String>,
        Option<i64>,
        Option<i64>,
        Option<i64>,
        Option<String>,
    );
    let mut stmt = guard
        .prepare(
            "SELECT event_key, partition_key, partition_hash, partition_id, partition_count, \
             partition_strategy FROM events ORDER BY sequence",
        )
        .unwrap();
    let rows: Vec<PartitionRow> = stmt
        .query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(rows.len(), 100);
    for (i, row) in rows.iter().enumerate() {
        let expected_key = format!("k{i}");
        let expected_pk = PartitionKey::new(&expected_key).unwrap();
        let expected_hash = hasher.hash(&expected_pk);
        let expected_hash_i64 = expected_hash.to_sql_i64();
        let expected_partition_id = (expected_hash.get() % 4) as i64;

        assert_eq!(row.0, expected_key, "row {i} event_key");
        assert_eq!(row.1.as_deref(), Some(expected_key.as_str()), "row {i}");
        assert_eq!(row.2, Some(expected_hash_i64), "row {i} partition_hash");
        assert_eq!(row.3, Some(expected_partition_id), "row {i} partition_id");
        assert_eq!(row.4, Some(4_i64), "row {i} partition_count");
        assert_eq!(row.5.as_deref(), Some("fnv1a64:v1"), "row {i} strategy");
    }
}

#[tokio::test]
async fn sqlite_partition_backfill_is_idempotent_on_second_run() {
    let db = SqliteDatabase::open_in_memory().unwrap();
    SqliteWriter::prepare_schema(&db.conn(), &SqliteWriterConfig::default()).unwrap();
    let writer = SqliteWriter::new(db.conn());

    for i in 0..20 {
        writer
            .write(&event_with_key(&format!("k{i}")))
            .await
            .unwrap();
    }

    let make_config = || {
        SqlitePartitionBackfillConfig::new(
            SqliteRelationName::new("events").unwrap(),
            NonZeroU32::new(4).unwrap(),
            EventKeyPartitionKeyResolver::new(),
            Fnv1a64PartitionHasher,
            8,
        )
        .unwrap()
    };

    let first = SqlitePartitionBackfill::new(db.conn(), make_config())
        .run()
        .await
        .unwrap();
    assert_eq!(first.rows_updated, 20);

    let second = SqlitePartitionBackfill::new(db.conn(), make_config())
        .run()
        .await
        .unwrap();
    assert_eq!(second.rows_updated, 0);
    assert_eq!(second.batches, 0);
}

#[tokio::test]
async fn sqlite_partition_backfill_skips_already_partitioned_rows() {
    let db = SqliteDatabase::open_in_memory().unwrap();

    let inline_config = SqliteWriterConfig {
        partitioning: SqlitePartitioningConfig::inline(
            NonZeroU32::new(4).unwrap(),
            EventKeyPartitionKeyResolver::new(),
            Fnv1a64PartitionHasher,
        ),
        ..SqliteWriterConfig::default()
    };
    SqliteWriter::prepare_schema(&db.conn(), &inline_config).unwrap();
    let inline_writer = SqliteWriter::new_with_config(db.conn(), inline_config);
    let off_writer = SqliteWriter::new(db.conn());

    for i in 0..5 {
        inline_writer
            .write(&event_with_key(&format!("pre{i}")))
            .await
            .unwrap();
    }
    for i in 0..5 {
        off_writer
            .write(&event_with_key(&format!("null{i}")))
            .await
            .unwrap();
    }

    let config = SqlitePartitionBackfillConfig::new(
        SqliteRelationName::new("events").unwrap(),
        NonZeroU32::new(4).unwrap(),
        EventKeyPartitionKeyResolver::new(),
        Fnv1a64PartitionHasher,
        10,
    )
    .unwrap();
    let report = SqlitePartitionBackfill::new(db.conn(), config)
        .run()
        .await
        .unwrap();
    assert_eq!(report.rows_updated, 5);

    let conn = db.conn();
    let guard = conn.lock().unwrap();
    let null_count: i64 = guard
        .query_row(
            "SELECT COUNT(*) FROM events WHERE partition_id IS NULL",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(null_count, 0);
}

#[test]
fn sqlite_partition_backfill_config_rejects_zero_batch_size() {
    let err = SqlitePartitionBackfillConfig::new(
        SqliteRelationName::new("events").unwrap(),
        NonZeroU32::new(4).unwrap(),
        EventKeyPartitionKeyResolver::new(),
        Fnv1a64PartitionHasher,
        0,
    )
    .unwrap_err();

    assert!(matches!(err, Error::Config(message) if message.contains("batch size")));
}
