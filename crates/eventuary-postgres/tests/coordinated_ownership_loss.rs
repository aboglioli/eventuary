use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use sqlx::PgPool;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::time::timeout;

use eventuary_core::io::reader::CheckpointScope;
use eventuary_core::io::{ConsumerGroupId, OwnerId, Reader, StreamId, Writer};
use eventuary_core::partition::{EventKeyPartitionKeyResolver, Fnv1a64PartitionHasher};
use eventuary_core::{Event, Error, Payload, StartFrom};
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
    PgWriter::prepare_schema(&pool, &PgWriterConfig::default())
        .await
        .unwrap();
    PgPartitionCoordinator::prepare_schema(&pool, &PgPartitionCoordinatorConfig::default())
        .await
        .unwrap();
    (container, pool)
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_coordinated_reader_ack_fails_with_ownership_lost_after_lease_stolen() {
    let (_c, pool) = start_postgres().await;
    let partition_count = NonZeroU16::new(1).unwrap();

    let writer = PgWriter::new_with_config(
        pool.clone(),
        PgWriterConfig {
            partitioning: PgPartitioningConfig::inline(
                partition_count,
                EventKeyPartitionKeyResolver::new(),
                Fnv1a64PartitionHasher,
            ),
            ..PgWriterConfig::default()
        },
    );

    writer.write(&event_with_key("event-1")).await.unwrap();
    writer.write(&event_with_key("event-2")).await.unwrap();

    let coordinator = Arc::new(PgPartitionCoordinator::new(
        pool.clone(),
        PgPartitionCoordinatorConfig::default(),
    ));

    let scope = CheckpointScope::new(
        ConsumerGroupId::new("ownership-loss-group").unwrap(),
        StreamId::new("orders").unwrap(),
    );

    let reader_a = PgCoordinatedReader::new(
        PgReader::new(pool.clone(), PgReaderConfig::default()),
        Arc::clone(&coordinator),
        OwnerId::new("owner-a").unwrap(),
        PgCoordinatedReaderConfig {
            partition_lease_duration: Duration::from_secs(2),
            partition_renew_interval: Duration::from_secs(60),
            consumer_lease_duration: Duration::from_secs(10),
            consumer_heartbeat_interval: Duration::from_secs(60),
            rebalance_interval: Duration::from_secs(60),
            partition_slack: 0,
        },
    );

    let mut stream_a = reader_a
        .read(PgCoordinatedSubscription {
            scope: scope.clone(),
            partition_count,
            start: StartFrom::Earliest,
            inner: PgSubscription::default(),
        })
        .await
        .unwrap();

    let msg_a = timeout(Duration::from_secs(5), stream_a.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(msg_a.event().key().as_str(), "event-1");

    tokio::time::sleep(Duration::from_secs(3)).await;

    let reader_b = PgCoordinatedReader::new(
        PgReader::new(pool.clone(), PgReaderConfig::default()),
        Arc::clone(&coordinator),
        OwnerId::new("owner-b").unwrap(),
        PgCoordinatedReaderConfig {
            partition_lease_duration: Duration::from_secs(60),
            partition_renew_interval: Duration::from_secs(15),
            consumer_lease_duration: Duration::from_secs(30),
            consumer_heartbeat_interval: Duration::from_secs(10),
            rebalance_interval: Duration::from_millis(100),
            partition_slack: 0,
        },
    );

    let mut stream_b = reader_b
        .read(PgCoordinatedSubscription {
            scope: scope.clone(),
            partition_count,
            start: StartFrom::Earliest,
            inner: PgSubscription::default(),
        })
        .await
        .unwrap();

    let msg_b = timeout(Duration::from_secs(5), stream_b.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(msg_b.event().key().as_str(), "event-1");
    msg_b.ack().await.unwrap();

    let ack_result = msg_a.ack().await;
    assert!(
        matches!(ack_result, Err(Error::OwnershipLost(_))),
        "expected OwnershipLost after lease stolen; got {ack_result:?}"
    );
}
