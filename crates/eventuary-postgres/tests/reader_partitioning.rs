use std::num::NonZeroU32;

use futures::StreamExt;
use sqlx::PgPool;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::time::timeout;

use eventuary_core::io::{Acker, Reader, Writer};
use eventuary_core::partition::{
    EventKeyPartitionKeyResolver, Fnv1a64PartitionHasher, PartitionHasher, PartitionKey,
    PartitionSelection,
};
use eventuary_core::{Event, Partition, Payload, StartFrom, StopAt};
use eventuary_postgres::PgPartitioningConfig;
use eventuary_postgres::database::PgDatabase;
use eventuary_postgres::reader::{PgReader, PgReaderConfig, PgSubscription};
use eventuary_postgres::writer::{PgWriter, PgWriterConfig};

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

const PARTITION_COUNT: u32 = 4;

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

fn partition_for_key(key: &str) -> u32 {
    let k = PartitionKey::new(key).unwrap();
    let hash = Fnv1a64PartitionHasher.hash(&k);
    (hash.get() % PARTITION_COUNT as u64) as u32
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_reader_default_all_returns_every_event() {
    let (_c, pool) = start_postgres().await;

    let writer_config = PgWriterConfig {
        partitioning: PgPartitioningConfig::inline(
            NonZeroU32::new(PARTITION_COUNT).unwrap(),
            EventKeyPartitionKeyResolver::new(),
            Fnv1a64PartitionHasher,
        ),
        ..PgWriterConfig::default()
    };
    let writer = PgWriter::new_with_config(pool.clone(), writer_config);

    let keys = [
        "key-a", "key-b", "key-c", "key-d", "key-e", "key-f", "key-g", "key-h",
    ];
    for key in &keys {
        writer.write(&event_with_key(key)).await.unwrap();
    }

    let reader = PgReader::new(pool.clone(), PgReaderConfig::default());
    let subscription = PgSubscription {
        start: StartFrom::Earliest,
        stop_at: StopAt::CurrentEnd,
        ..PgSubscription::default()
    };
    let mut stream = reader.read(subscription).await.unwrap();

    let mut count = 0usize;
    while let Ok(Some(Ok(msg))) = timeout(std::time::Duration::from_secs(5), stream.next()).await {
        msg.acker().ack().await.unwrap();
        count += 1;
    }

    assert_eq!(count, keys.len());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_reader_one_filters_to_single_partition() {
    let (_c, pool) = start_postgres().await;

    let writer_config = PgWriterConfig {
        partitioning: PgPartitioningConfig::inline(
            NonZeroU32::new(PARTITION_COUNT).unwrap(),
            EventKeyPartitionKeyResolver::new(),
            Fnv1a64PartitionHasher,
        ),
        ..PgWriterConfig::default()
    };
    let writer = PgWriter::new_with_config(pool.clone(), writer_config);

    let keys = [
        "key-a", "key-b", "key-c", "key-d", "key-e", "key-f", "key-g", "key-h",
    ];
    for key in &keys {
        writer.write(&event_with_key(key)).await.unwrap();
    }

    let partitions_by_id: std::collections::HashMap<u32, Vec<&str>> =
        keys.iter()
            .fold(std::collections::HashMap::new(), |mut acc, key| {
                acc.entry(partition_for_key(key)).or_default().push(key);
                acc
            });

    let (chosen_partition, expected_keys) = partitions_by_id
        .iter()
        .find(|(_, ks)| ks.len() >= 2)
        .map(|(id, ks)| (*id, ks.clone()))
        .expect("expected at least one partition with >=2 events");

    let reader = PgReader::new(pool.clone(), PgReaderConfig::default());
    let subscription = PgSubscription {
        start: StartFrom::Earliest,
        stop_at: StopAt::CurrentEnd,
        partitions: PartitionSelection::One(
            Partition::new(chosen_partition, NonZeroU32::new(PARTITION_COUNT).unwrap()).unwrap(),
        ),
        ..PgSubscription::default()
    };
    let mut stream = reader.read(subscription).await.unwrap();

    let mut received_keys: Vec<String> = Vec::new();
    while let Ok(Some(Ok(msg))) = timeout(std::time::Duration::from_secs(5), stream.next()).await {
        let key = msg.event().key().as_str().to_owned();
        msg.acker().ack().await.unwrap();
        received_keys.push(key);
    }

    assert_eq!(received_keys.len(), expected_keys.len());
    for key in &received_keys {
        assert_eq!(
            partition_for_key(key),
            chosen_partition,
            "event key {key} maps to wrong partition"
        );
    }
}
