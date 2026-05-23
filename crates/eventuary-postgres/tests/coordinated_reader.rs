use std::collections::{HashMap, HashSet};
use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use sqlx::PgPool;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::time::timeout;

use eventuary_core::io::{ConsumerGroupId, OwnerId, Reader, StreamId, Writer};
use eventuary_core::partition::{EventKeyPartitionKeyResolver, Fnv1a64PartitionHasher};
use eventuary_core::{Event, Payload, StartFrom};
use eventuary_postgres::database::PgDatabase;
use eventuary_postgres::reader::{PgReader, PgReaderConfig, PgSubscription};
use eventuary_postgres::{
    PgCoordinatedReader, PgCoordinatedReaderConfig, PgCoordinatedSubscription,
    PgPartitionCoordinator, PgPartitionCoordinatorConfig, PgPartitioningConfig, PgWriter,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_coordinated_reader_two_owners_claim_disjoint_partitions() {
    let (_c, pool) = start_postgres().await;

    let partition_count = NonZeroU16::new(4).unwrap();

    let writer_config = PgWriterConfig {
        partitioning: PgPartitioningConfig::inline(
            partition_count,
            EventKeyPartitionKeyResolver::event_id_on_unkeyed(),
            Fnv1a64PartitionHasher,
        ),
        ..PgWriterConfig::default()
    };
    let writer = PgWriter::new_with_config(pool.clone(), writer_config);

    let keys = ["k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7"];
    for key in &keys {
        writer.write(&event_with_key(key)).await.unwrap();
    }

    let coordinator = Arc::new(PgPartitionCoordinator::new(
        pool.clone(),
        PgPartitionCoordinatorConfig::default(),
    ));

    let reader_config = PgCoordinatedReaderConfig {
        partition_lease_duration: Duration::from_secs(60),
        partition_renew_interval: Duration::from_secs(15),
        consumer_lease_duration: Duration::from_secs(30),
        consumer_heartbeat_interval: Duration::from_secs(10),
    };

    let group = ConsumerGroupId::new("test-group").unwrap();
    let stream_id = StreamId::new("orders").unwrap();

    let reader_a = PgCoordinatedReader::new(
        PgReader::new(pool.clone(), PgReaderConfig::default()),
        Arc::clone(&coordinator),
        OwnerId::new("owner-a").unwrap(),
        PgCoordinatedReaderConfig {
            partition_lease_duration: reader_config.partition_lease_duration,
            partition_renew_interval: reader_config.partition_renew_interval,
            consumer_lease_duration: reader_config.consumer_lease_duration,
            consumer_heartbeat_interval: reader_config.consumer_heartbeat_interval,
        },
    );

    let reader_b = PgCoordinatedReader::new(
        PgReader::new(pool.clone(), PgReaderConfig::default()),
        Arc::clone(&coordinator),
        OwnerId::new("owner-b").unwrap(),
        PgCoordinatedReaderConfig {
            partition_lease_duration: reader_config.partition_lease_duration,
            partition_renew_interval: reader_config.partition_renew_interval,
            consumer_lease_duration: reader_config.consumer_lease_duration,
            consumer_heartbeat_interval: reader_config.consumer_heartbeat_interval,
        },
    );

    let sub_a = PgCoordinatedSubscription {
        consumer_group_id: group.clone(),
        stream_id: stream_id.clone(),
        partition_count,
        start: StartFrom::Earliest,
        inner: PgSubscription {
            start: StartFrom::Earliest,
            ..PgSubscription::default()
        },
    };

    let sub_b = PgCoordinatedSubscription {
        consumer_group_id: group.clone(),
        stream_id: stream_id.clone(),
        partition_count,
        start: StartFrom::Earliest,
        inner: PgSubscription {
            start: StartFrom::Earliest,
            ..PgSubscription::default()
        },
    };

    let (stream_a, stream_b) = tokio::join!(reader_a.read(sub_a), reader_b.read(sub_b));

    let stream_a = stream_a.unwrap();
    let stream_b = stream_b.unwrap();

    let drain = |mut stream: eventuary_core::io::stream::SpawnedStream<
        eventuary_postgres::PgCoordinatedAcker,
        eventuary_postgres::PgPartitionCursor,
    >| async move {
        let mut results: HashMap<u16, Vec<String>> = HashMap::new();
        while let Ok(Some(Ok(msg))) = timeout(Duration::from_secs(2), stream.next()).await {
            let partition_id = msg.cursor().partition_id();
            let key = msg
                .event()
                .key()
                .map(|k| k.as_str().to_owned())
                .unwrap_or_default();
            msg.ack().await.unwrap();
            results.entry(partition_id).or_default().push(key);
        }
        results
    };

    let (results_a, results_b) = tokio::join!(drain(stream_a), drain(stream_b));

    let total_a: usize = results_a.values().map(|v| v.len()).sum();
    let total_b: usize = results_b.values().map(|v| v.len()).sum();
    assert_eq!(
        total_a + total_b,
        keys.len(),
        "combined count must be 8; got a={total_a} b={total_b}"
    );

    let partitions_a: HashSet<u16> = results_a.keys().copied().collect();
    let partitions_b: HashSet<u16> = results_b.keys().copied().collect();
    let overlap: HashSet<u16> = partitions_a.intersection(&partitions_b).copied().collect();
    assert!(
        overlap.is_empty(),
        "partitions should be disjoint; overlap={overlap:?}"
    );
}
