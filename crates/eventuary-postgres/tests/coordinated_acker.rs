use std::sync::Arc;

use sqlx::{PgPool, Row};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

use eventuary_core::io::{Acker, ConsumerGroupId, OwnerId, PartitionCoordinator, StreamId};
use eventuary_postgres::database::PgDatabase;
use eventuary_postgres::reader::PgCursorAcker;
use eventuary_postgres::{
    PgCoordinatedAcker, PgPartitionCoordinator, PgPartitionCoordinatorConfig,
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

fn coordinator(pool: PgPool) -> PgPartitionCoordinator {
    PgPartitionCoordinator::new(pool, PgPartitionCoordinatorConfig::default())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acker_ack_advances_checkpoint() {
    let (_c, pool) = start_postgres().await;
    let coord = coordinator(pool.clone());
    let coord_arc = Arc::new(coord);

    let group = ConsumerGroupId::new("group-1").unwrap();
    let stream = StreamId::new("orders").unwrap();
    let owner_a = OwnerId::new("worker-a").unwrap();
    let lease_dur = std::time::Duration::from_secs(60);

    coord_arc
        .heartbeat(&group, &stream, &owner_a, lease_dur)
        .await
        .unwrap();
    let lease = coord_arc
        .claim(&group, &stream, 7, &owner_a, lease_dur)
        .await
        .unwrap()
        .expect("claim succeeded");

    let acker = PgCoordinatedAcker::new(
        PgCursorAcker::dummy(50),
        Arc::clone(&coord_arc),
        group.clone(),
        stream.clone(),
        7,
        owner_a.clone(),
        lease.generation,
        50,
    );

    acker.ack().await.unwrap();

    let row = sqlx::query(
        "SELECT checkpoint_sequence FROM event_stream_partitions \
         WHERE consumer_group_id = $1 AND stream_id = $2 AND partition_id = $3",
    )
    .bind(group.as_str())
    .bind(stream.as_str())
    .bind(7_i32)
    .fetch_one(&pool)
    .await
    .unwrap();

    let checkpoint: i64 = row.get("checkpoint_sequence");
    assert_eq!(checkpoint, 50);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acker_ack_fails_after_partition_taken_over() {
    let (_c, pool) = start_postgres().await;
    let coord = coordinator(pool.clone());
    let coord_arc = Arc::new(coord);

    let group = ConsumerGroupId::new("group-1").unwrap();
    let stream = StreamId::new("orders").unwrap();
    let owner_a = OwnerId::new("worker-a").unwrap();
    let owner_b = OwnerId::new("worker-b").unwrap();
    let short_lease = std::time::Duration::from_millis(50);
    let long_lease = std::time::Duration::from_secs(60);

    coord_arc
        .heartbeat(&group, &stream, &owner_a, long_lease)
        .await
        .unwrap();
    let lease_a = coord_arc
        .claim(&group, &stream, 7, &owner_a, short_lease)
        .await
        .unwrap()
        .expect("owner a claims");
    assert_eq!(lease_a.generation, 1);

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    coord_arc
        .heartbeat(&group, &stream, &owner_b, long_lease)
        .await
        .unwrap();
    coord_arc
        .claim(&group, &stream, 7, &owner_b, long_lease)
        .await
        .unwrap()
        .expect("owner b takes over");

    let stale_acker = PgCoordinatedAcker::new(
        PgCursorAcker::dummy(50),
        Arc::clone(&coord_arc),
        group.clone(),
        stream.clone(),
        7,
        owner_a.clone(),
        lease_a.generation,
        50,
    );

    let err = stale_acker.ack().await.unwrap_err();
    assert!(matches!(err, eventuary_core::Error::OwnershipLost(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acker_nack_does_not_touch_checkpoint() {
    let (_c, pool) = start_postgres().await;
    let coord = coordinator(pool.clone());
    let coord_arc = Arc::new(coord);

    let group = ConsumerGroupId::new("group-1").unwrap();
    let stream = StreamId::new("orders").unwrap();
    let owner_a = OwnerId::new("worker-a").unwrap();
    let lease_dur = std::time::Duration::from_secs(60);

    coord_arc
        .heartbeat(&group, &stream, &owner_a, lease_dur)
        .await
        .unwrap();
    let lease = coord_arc
        .claim(&group, &stream, 7, &owner_a, lease_dur)
        .await
        .unwrap()
        .expect("claim succeeded");

    let acker = PgCoordinatedAcker::new(
        PgCursorAcker::dummy(100),
        Arc::clone(&coord_arc),
        group.clone(),
        stream.clone(),
        7,
        owner_a.clone(),
        lease.generation,
        100,
    );

    acker.nack().await.unwrap();

    let row = sqlx::query(
        "SELECT checkpoint_sequence FROM event_stream_partitions \
         WHERE consumer_group_id = $1 AND stream_id = $2 AND partition_id = $3",
    )
    .bind(group.as_str())
    .bind(stream.as_str())
    .bind(7_i32)
    .fetch_one(&pool)
    .await
    .unwrap();

    let checkpoint: i64 = row.get("checkpoint_sequence");
    assert_eq!(checkpoint, 0);
}
