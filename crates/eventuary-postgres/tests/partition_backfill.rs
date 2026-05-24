use std::num::NonZeroU16;

use sqlx::PgPool;
use sqlx::Row;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

use eventuary_core::io::Writer;
use eventuary_core::partition::{
    EventKeyPartitionKeyResolver, Fnv1a64PartitionHasher, PartitionHasher,
};
use eventuary_core::{Event, Payload};
use eventuary_postgres::database::PgDatabase;
use eventuary_postgres::relation::PgRelationName;
use eventuary_postgres::{
    BackfillReport, PgPartitionBackfill, PgPartitionBackfillConfig, PgPartitioningConfig, PgWriter,
    PgWriterConfig,
};

async fn start_postgres() -> (ContainerAsync<GenericImage>, PgPool) {
    let container = GenericImage::new("postgres", "18-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "eventuary")
        .with_env_var("POSTGRES_PASSWORD", "eventuary")
        .with_env_var("POSTGRES_DB", "eventuary")
        .start()
        .await
        .expect("postgres start");
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://eventuary:eventuary@127.0.0.1:{port}/eventuary");
    let db = PgDatabase::connect(&url).await.unwrap();
    let pool = db.pool();
    (container, pool)
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_partition_backfill_populates_partition_columns() {
    let (_c, pool) = start_postgres().await;
    let writer = PgWriter::new(pool.clone());

    for i in 0..10 {
        writer
            .write(&event_with_key(&format!("k{i}")))
            .await
            .unwrap();
    }

    let config = PgPartitionBackfillConfig::new(
        PgRelationName::new("events").unwrap(),
        NonZeroU16::new(4).unwrap(),
        EventKeyPartitionKeyResolver::event_id_on_unkeyed(),
        Fnv1a64PartitionHasher,
        3,
    );
    let backfill = PgPartitionBackfill::new(pool.clone(), config);
    let report: BackfillReport = backfill.run().await.unwrap();

    assert_eq!(report.rows_updated, 10);
    assert!(
        report.batches >= 4,
        "expected >=4 batches, got {}",
        report.batches
    );

    let hasher = Fnv1a64PartitionHasher;
    let rows = sqlx::query(
        "SELECT event_key, partition_key, partition_hash, partition_id, partition_count, partition_strategy \
         FROM events ORDER BY sequence",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(rows.len(), 10);

    for (i, row) in rows.iter().enumerate() {
        let expected_key = format!("k{i}");
        let expected_hash_u64 = hasher.hash(&expected_key);
        let expected_hash_i64 = expected_hash_u64 as i64;
        let expected_partition_id = (expected_hash_u64 % 4) as i32;

        let partition_key: Option<String> = row.get("partition_key");
        let partition_hash: Option<i64> = row.get("partition_hash");
        let partition_id: Option<i32> = row.get("partition_id");
        let partition_count: Option<i32> = row.get("partition_count");
        let partition_strategy: Option<String> = row.get("partition_strategy");

        assert_eq!(
            partition_key.as_deref(),
            Some(expected_key.as_str()),
            "row {i}"
        );
        assert_eq!(partition_hash, Some(expected_hash_i64), "row {i}");
        assert_eq!(partition_id, Some(expected_partition_id), "row {i}");
        assert_eq!(partition_count, Some(4_i32), "row {i}");
        assert_eq!(partition_strategy.as_deref(), Some("fnv1a64:v1"), "row {i}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_partition_backfill_skips_already_partitioned_rows() {
    let (_c, pool) = start_postgres().await;

    let inline_config = PgWriterConfig {
        partitioning: PgPartitioningConfig::inline(
            NonZeroU16::new(4).unwrap(),
            EventKeyPartitionKeyResolver::event_id_on_unkeyed(),
            Fnv1a64PartitionHasher,
        ),
        ..PgWriterConfig::default()
    };
    let inline_writer = PgWriter::new_with_config(pool.clone(), inline_config);
    let off_writer = PgWriter::new(pool.clone());

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

    let config = PgPartitionBackfillConfig::new(
        PgRelationName::new("events").unwrap(),
        NonZeroU16::new(4).unwrap(),
        EventKeyPartitionKeyResolver::event_id_on_unkeyed(),
        Fnv1a64PartitionHasher,
        10,
    );
    let backfill = PgPartitionBackfill::new(pool.clone(), config);
    let report = backfill.run().await.unwrap();

    assert_eq!(report.rows_updated, 5);

    let null_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE partition_id IS NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(null_count, 0);
}
