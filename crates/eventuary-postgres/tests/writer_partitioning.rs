use std::num::NonZeroU32;

use sqlx::PgPool;
use sqlx::Row;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

use eventuary_core::io::Writer;
use eventuary_core::partition::{EventKeyPartitionKeyResolver, Fnv1a64PartitionHasher};
use eventuary_core::{Event, Payload};
use eventuary_postgres::database::PgDatabase;
use eventuary_postgres::writer::{PgPartitioningConfig, PgWriter, PgWriterConfig};

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
    PgWriter::prepare_schema(&pool, &PgWriterConfig::default())
        .await
        .unwrap();
    (container, pool)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_writer_off_partitioning_leaves_columns_null() {
    let (_c, pool) = start_postgres().await;
    let writer = PgWriter::new(pool.clone());

    let event = Event::builder(
        "acme",
        "/orders",
        "order.placed",
        "no-partition",
        Payload::from_string("{}"),
    )
    .unwrap()
    .build()
    .unwrap();
    writer.write(&event).await.unwrap();

    let row = sqlx::query(
        "SELECT partition_key, partition_hash, partition_id, partition_count, partition_strategy \
         FROM events LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let partition_key: Option<String> = row.get("partition_key");
    let partition_hash: Option<i64> = row.get("partition_hash");
    let partition_id: Option<i64> = row.get("partition_id");
    let partition_count: Option<i64> = row.get("partition_count");
    let partition_strategy: Option<String> = row.get("partition_strategy");

    assert!(partition_key.is_none());
    assert!(partition_hash.is_none());
    assert!(partition_id.is_none());
    assert!(partition_count.is_none());
    assert!(partition_strategy.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_writer_inline_partitioning_persists_all_columns() {
    let (_c, pool) = start_postgres().await;

    let config = PgWriterConfig {
        partitioning: PgPartitioningConfig::inline(
            NonZeroU32::new(64).unwrap(),
            EventKeyPartitionKeyResolver::new(),
            Fnv1a64PartitionHasher,
        ),
        ..PgWriterConfig::default()
    };
    let writer = PgWriter::new_with_config(pool.clone(), config);

    let event = Event::builder(
        "acme",
        "/orders",
        "order.placed",
        "order-123",
        Payload::from_string("{}"),
    )
    .unwrap()
    .build()
    .unwrap();
    writer.write(&event).await.unwrap();

    let row = sqlx::query(
        "SELECT partition_key, partition_hash, partition_id, partition_count, partition_strategy \
         FROM events LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let partition_key: Option<String> = row.get("partition_key");
    let partition_hash: Option<i64> = row.get("partition_hash");
    let partition_id: Option<i64> = row.get("partition_id");
    let partition_count: Option<i64> = row.get("partition_count");
    let partition_strategy: Option<String> = row.get("partition_strategy");

    let expected_hash_u64: u64 = 0x1b96f9c28b5d5aba;
    let expected_hash_i64 = expected_hash_u64 as i64;
    let expected_partition_id = (expected_hash_u64 % 64) as i64;

    assert_eq!(partition_key.as_deref(), Some("order-123"));
    assert_eq!(partition_hash, Some(expected_hash_i64));
    assert_eq!(partition_id, Some(expected_partition_id));
    assert_eq!(partition_count, Some(64_i64));
    assert_eq!(partition_strategy.as_deref(), Some("fnv1a64:v1"));
}
