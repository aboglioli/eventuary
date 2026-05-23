use sqlx::PgPool;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

use eventuary_core::io::{ConsumerGroupId, OwnerId, PartitionCoordinator, StreamId};
use eventuary_postgres::database::PgDatabase;
use eventuary_postgres::{PgPartitionCoordinator, PgPartitionCoordinatorConfig};

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
async fn pg_coordinator_heartbeat_and_live_count() {
    let (_c, pool) = start_postgres().await;
    let coord = coordinator(pool);

    let group = ConsumerGroupId::new("group-1").unwrap();
    let stream = StreamId::new("orders").unwrap();
    let owner_a = OwnerId::new("worker-a").unwrap();
    let owner_b = OwnerId::new("worker-b").unwrap();
    let owner_c = OwnerId::new("worker-c").unwrap();

    let long_lease = std::time::Duration::from_secs(60);
    let short_lease = std::time::Duration::from_millis(50);

    coord
        .heartbeat(&group, &stream, &owner_a, long_lease)
        .await
        .unwrap();
    coord
        .heartbeat(&group, &stream, &owner_b, long_lease)
        .await
        .unwrap();
    coord
        .heartbeat(&group, &stream, &owner_c, short_lease)
        .await
        .unwrap();

    let live = coord.live_consumers(&group, &stream).await.unwrap();
    assert_eq!(live, 3);

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let live_after = coord.live_consumers(&group, &stream).await.unwrap();
    assert_eq!(live_after, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_coordinator_claim_free_partition_succeeds() {
    let (_c, pool) = start_postgres().await;
    let coord = coordinator(pool);

    let group = ConsumerGroupId::new("group-1").unwrap();
    let stream = StreamId::new("orders").unwrap();
    let owner = OwnerId::new("worker-a").unwrap();
    let lease_dur = std::time::Duration::from_secs(60);

    let result = coord
        .claim(&group, &stream, 0, &owner, lease_dur)
        .await
        .unwrap();
    let lease = result.expect("should get lease");

    assert_eq!(lease.generation, 1);
    assert_eq!(lease.checkpoint_sequence, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_coordinator_claim_contested_returns_none() {
    let (_c, pool) = start_postgres().await;
    let coord = coordinator(pool);

    let group = ConsumerGroupId::new("group-1").unwrap();
    let stream = StreamId::new("orders").unwrap();
    let owner_a = OwnerId::new("worker-a").unwrap();
    let owner_b = OwnerId::new("worker-b").unwrap();
    let lease_dur = std::time::Duration::from_secs(60);

    let first = coord
        .claim(&group, &stream, 0, &owner_a, lease_dur)
        .await
        .unwrap();
    assert!(first.is_some());

    let second = coord
        .claim(&group, &stream, 0, &owner_b, lease_dur)
        .await
        .unwrap();
    assert!(second.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_coordinator_claim_after_expiry_succeeds() {
    let (_c, pool) = start_postgres().await;
    let coord = coordinator(pool);

    let group = ConsumerGroupId::new("group-1").unwrap();
    let stream = StreamId::new("orders").unwrap();
    let owner_a = OwnerId::new("worker-a").unwrap();
    let owner_b = OwnerId::new("worker-b").unwrap();
    let short_lease = std::time::Duration::from_millis(50);
    let long_lease = std::time::Duration::from_secs(60);

    let first = coord
        .claim(&group, &stream, 0, &owner_a, short_lease)
        .await
        .unwrap();
    assert_eq!(first.unwrap().generation, 1);

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let second = coord
        .claim(&group, &stream, 0, &owner_b, long_lease)
        .await
        .unwrap();
    let lease = second.expect("should succeed after expiry");
    assert_eq!(lease.generation, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_coordinator_renew_with_matching_generation() {
    let (_c, pool) = start_postgres().await;
    let coord = coordinator(pool);

    let group = ConsumerGroupId::new("group-1").unwrap();
    let stream = StreamId::new("orders").unwrap();
    let owner = OwnerId::new("worker-a").unwrap();
    let lease_dur = std::time::Duration::from_secs(60);

    let lease = coord
        .claim(&group, &stream, 0, &owner, lease_dur)
        .await
        .unwrap()
        .unwrap();

    coord
        .renew(&group, &stream, 0, &owner, lease.generation, lease_dur)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_coordinator_renew_with_stale_generation_returns_ownership_lost() {
    let (_c, pool) = start_postgres().await;
    let coord = coordinator(pool);

    let group = ConsumerGroupId::new("group-1").unwrap();
    let stream = StreamId::new("orders").unwrap();
    let owner_a = OwnerId::new("worker-a").unwrap();
    let owner_b = OwnerId::new("worker-b").unwrap();
    let short_lease = std::time::Duration::from_millis(50);
    let long_lease = std::time::Duration::from_secs(60);

    let lease_a = coord
        .claim(&group, &stream, 0, &owner_a, short_lease)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(lease_a.generation, 1);

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    coord
        .claim(&group, &stream, 0, &owner_b, long_lease)
        .await
        .unwrap()
        .unwrap();

    let err = coord
        .renew(&group, &stream, 0, &owner_a, lease_a.generation, long_lease)
        .await
        .unwrap_err();
    assert!(matches!(err, eventuary_core::Error::OwnershipLost(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_coordinator_release_with_matching_generation() {
    let (_c, pool) = start_postgres().await;
    let coord = coordinator(pool);

    let group = ConsumerGroupId::new("group-1").unwrap();
    let stream = StreamId::new("orders").unwrap();
    let owner_a = OwnerId::new("worker-a").unwrap();
    let owner_b = OwnerId::new("worker-b").unwrap();
    let lease_dur = std::time::Duration::from_secs(60);

    let lease = coord
        .claim(&group, &stream, 0, &owner_a, lease_dur)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(lease.generation, 1);

    coord
        .release(&group, &stream, 0, &owner_a, lease.generation)
        .await
        .unwrap();

    let new_lease = coord
        .claim(&group, &stream, 0, &owner_b, lease_dur)
        .await
        .unwrap()
        .expect("should claim after release");
    assert_eq!(new_lease.generation, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_coordinator_release_with_stale_generation_returns_ownership_lost() {
    let (_c, pool) = start_postgres().await;
    let coord = coordinator(pool);

    let group = ConsumerGroupId::new("group-1").unwrap();
    let stream = StreamId::new("orders").unwrap();
    let owner_a = OwnerId::new("worker-a").unwrap();
    let owner_b = OwnerId::new("worker-b").unwrap();
    let short_lease = std::time::Duration::from_millis(50);
    let long_lease = std::time::Duration::from_secs(60);

    let lease_a = coord
        .claim(&group, &stream, 0, &owner_a, short_lease)
        .await
        .unwrap()
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    coord
        .claim(&group, &stream, 0, &owner_b, long_lease)
        .await
        .unwrap()
        .unwrap();

    let err = coord
        .release(&group, &stream, 0, &owner_a, lease_a.generation)
        .await
        .unwrap_err();
    assert!(matches!(err, eventuary_core::Error::OwnershipLost(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_coordinator_checkpoint_with_matching_generation_advances() {
    let (_c, pool) = start_postgres().await;
    let coord = coordinator(pool);

    let group = ConsumerGroupId::new("group-1").unwrap();
    let stream = StreamId::new("orders").unwrap();
    let owner_a = OwnerId::new("worker-a").unwrap();
    let owner_b = OwnerId::new("worker-b").unwrap();
    let long_lease = std::time::Duration::from_secs(60);

    let lease = coord
        .claim(&group, &stream, 0, &owner_a, long_lease)
        .await
        .unwrap()
        .unwrap();

    coord
        .checkpoint(&group, &stream, 0, &owner_a, lease.generation, 100)
        .await
        .unwrap();

    coord
        .release(&group, &stream, 0, &owner_a, lease.generation)
        .await
        .unwrap();

    let new_lease = coord
        .claim(&group, &stream, 0, &owner_b, long_lease)
        .await
        .unwrap()
        .expect("should claim after release");
    assert_eq!(new_lease.checkpoint_sequence, 100);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_coordinator_checkpoint_is_monotonic() {
    let (_c, pool) = start_postgres().await;
    let coord = coordinator(pool);

    let group = ConsumerGroupId::new("group-1").unwrap();
    let stream = StreamId::new("orders").unwrap();
    let owner_a = OwnerId::new("worker-a").unwrap();
    let owner_b = OwnerId::new("worker-b").unwrap();
    let long_lease = std::time::Duration::from_secs(60);

    let lease = coord
        .claim(&group, &stream, 0, &owner_a, long_lease)
        .await
        .unwrap()
        .unwrap();

    coord
        .checkpoint(&group, &stream, 0, &owner_a, lease.generation, 100)
        .await
        .unwrap();

    coord
        .checkpoint(&group, &stream, 0, &owner_a, lease.generation, 50)
        .await
        .unwrap();

    coord
        .release(&group, &stream, 0, &owner_a, lease.generation)
        .await
        .unwrap();

    let new_lease = coord
        .claim(&group, &stream, 0, &owner_b, long_lease)
        .await
        .unwrap()
        .expect("should claim after release");
    assert_eq!(new_lease.checkpoint_sequence, 100);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_coordinator_checkpoint_with_stale_generation_returns_ownership_lost() {
    let (_c, pool) = start_postgres().await;
    let coord = coordinator(pool);

    let group = ConsumerGroupId::new("group-1").unwrap();
    let stream = StreamId::new("orders").unwrap();
    let owner_a = OwnerId::new("worker-a").unwrap();
    let owner_b = OwnerId::new("worker-b").unwrap();
    let short_lease = std::time::Duration::from_millis(50);
    let long_lease = std::time::Duration::from_secs(60);

    let lease_a = coord
        .claim(&group, &stream, 0, &owner_a, short_lease)
        .await
        .unwrap()
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    coord
        .claim(&group, &stream, 0, &owner_b, long_lease)
        .await
        .unwrap()
        .unwrap();

    let err = coord
        .checkpoint(&group, &stream, 0, &owner_a, lease_a.generation, 100)
        .await
        .unwrap_err();
    assert!(matches!(err, eventuary_core::Error::OwnershipLost(_)));
}
