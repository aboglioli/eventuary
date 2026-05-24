use std::num::NonZeroU16;

use sqlx::PgPool;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

use eventuary_core::Partition;
use eventuary_core::io::reader::CheckpointScope;
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

fn scope() -> CheckpointScope {
    CheckpointScope::new(
        ConsumerGroupId::new("group-1").unwrap(),
        StreamId::new("orders").unwrap(),
    )
}

fn partition(id: u16) -> Partition {
    Partition::new(id, NonZeroU16::new(64).unwrap()).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_coordinator_heartbeat_and_live_count() {
    let (_c, pool) = start_postgres().await;
    let coord = coordinator(pool);

    let s = scope();
    let owner_a = OwnerId::new("worker-a").unwrap();
    let owner_b = OwnerId::new("worker-b").unwrap();
    let owner_c = OwnerId::new("worker-c").unwrap();

    let long_lease = std::time::Duration::from_secs(60);
    let short_lease = std::time::Duration::from_millis(50);

    coord.heartbeat(&s, &owner_a, long_lease).await.unwrap();
    coord.heartbeat(&s, &owner_b, long_lease).await.unwrap();
    coord.heartbeat(&s, &owner_c, short_lease).await.unwrap();

    let live = coord.live_consumers(&s).await.unwrap();
    assert_eq!(live, 3);

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let live_after = coord.live_consumers(&s).await.unwrap();
    assert_eq!(live_after, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_coordinator_claim_free_partition_succeeds() {
    let (_c, pool) = start_postgres().await;
    let coord = coordinator(pool);

    let s = scope();
    let owner = OwnerId::new("worker-a").unwrap();
    let lease_dur = std::time::Duration::from_secs(60);

    let result = coord
        .claim(&s, &owner, partition(0), lease_dur)
        .await
        .unwrap();
    let lease = result.expect("should get lease");

    assert_eq!(lease.generation.get(), 1);
    assert_eq!(lease.checkpoint_cursor.unwrap().sequence, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_coordinator_claim_contested_returns_none() {
    let (_c, pool) = start_postgres().await;
    let coord = coordinator(pool);

    let s = scope();
    let owner_a = OwnerId::new("worker-a").unwrap();
    let owner_b = OwnerId::new("worker-b").unwrap();
    let lease_dur = std::time::Duration::from_secs(60);

    let first = coord
        .claim(&s, &owner_a, partition(0), lease_dur)
        .await
        .unwrap();
    assert!(first.is_some());

    let second = coord
        .claim(&s, &owner_b, partition(0), lease_dur)
        .await
        .unwrap();
    assert!(second.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_coordinator_claim_after_expiry_succeeds() {
    let (_c, pool) = start_postgres().await;
    let coord = coordinator(pool);

    let s = scope();
    let owner_a = OwnerId::new("worker-a").unwrap();
    let owner_b = OwnerId::new("worker-b").unwrap();
    let short_lease = std::time::Duration::from_millis(50);
    let long_lease = std::time::Duration::from_secs(60);

    let first = coord
        .claim(&s, &owner_a, partition(0), short_lease)
        .await
        .unwrap();
    assert_eq!(first.unwrap().generation.get(), 1);

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let second = coord
        .claim(&s, &owner_b, partition(0), long_lease)
        .await
        .unwrap();
    let lease = second.expect("should succeed after expiry");
    assert_eq!(lease.generation.get(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_coordinator_renew_with_matching_generation() {
    let (_c, pool) = start_postgres().await;
    let coord = coordinator(pool);

    let s = scope();
    let owner = OwnerId::new("worker-a").unwrap();
    let lease_dur = std::time::Duration::from_secs(60);

    let lease = coord
        .claim(&s, &owner, partition(0), lease_dur)
        .await
        .unwrap()
        .unwrap();
    coord.renew(&lease, lease_dur).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_coordinator_renew_with_stale_generation_returns_ownership_lost() {
    let (_c, pool) = start_postgres().await;
    let coord = coordinator(pool);

    let s = scope();
    let owner_a = OwnerId::new("worker-a").unwrap();
    let owner_b = OwnerId::new("worker-b").unwrap();
    let short_lease = std::time::Duration::from_millis(50);
    let long_lease = std::time::Duration::from_secs(60);

    let lease_a = coord
        .claim(&s, &owner_a, partition(0), short_lease)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(lease_a.generation.get(), 1);

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    coord
        .claim(&s, &owner_b, partition(0), long_lease)
        .await
        .unwrap()
        .unwrap();

    let err = coord.renew(&lease_a, long_lease).await.unwrap_err();
    assert!(matches!(err, eventuary_core::Error::OwnershipLost(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_coordinator_release_with_matching_generation() {
    let (_c, pool) = start_postgres().await;
    let coord = coordinator(pool);

    let s = scope();
    let owner_a = OwnerId::new("worker-a").unwrap();
    let owner_b = OwnerId::new("worker-b").unwrap();
    let lease_dur = std::time::Duration::from_secs(60);

    let lease = coord
        .claim(&s, &owner_a, partition(0), lease_dur)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(lease.generation.get(), 1);

    coord.release(&lease).await.unwrap();

    let new_lease = coord
        .claim(&s, &owner_b, partition(0), lease_dur)
        .await
        .unwrap()
        .expect("should claim after release");
    assert_eq!(new_lease.generation.get(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_coordinator_release_with_stale_generation_returns_ownership_lost() {
    let (_c, pool) = start_postgres().await;
    let coord = coordinator(pool);

    let s = scope();
    let owner_a = OwnerId::new("worker-a").unwrap();
    let owner_b = OwnerId::new("worker-b").unwrap();
    let short_lease = std::time::Duration::from_millis(50);
    let long_lease = std::time::Duration::from_secs(60);

    let lease_a = coord
        .claim(&s, &owner_a, partition(0), short_lease)
        .await
        .unwrap()
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    coord
        .claim(&s, &owner_b, partition(0), long_lease)
        .await
        .unwrap()
        .unwrap();

    let err = coord.release(&lease_a).await.unwrap_err();
    assert!(matches!(err, eventuary_core::Error::OwnershipLost(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_coordinator_checkpoint_with_matching_generation_advances() {
    use eventuary_postgres::reader::PgCursor;

    let (_c, pool) = start_postgres().await;
    let coord = coordinator(pool);

    let s = scope();
    let owner_a = OwnerId::new("worker-a").unwrap();
    let owner_b = OwnerId::new("worker-b").unwrap();
    let long_lease = std::time::Duration::from_secs(60);

    let lease = coord
        .claim(&s, &owner_a, partition(0), long_lease)
        .await
        .unwrap()
        .unwrap();

    coord.checkpoint(&lease, PgCursor::new(100)).await.unwrap();

    coord.release(&lease).await.unwrap();

    let new_lease = coord
        .claim(&s, &owner_b, partition(0), long_lease)
        .await
        .unwrap()
        .expect("should claim after release");
    assert_eq!(new_lease.checkpoint_cursor.unwrap().sequence, 100);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_coordinator_checkpoint_is_monotonic() {
    use eventuary_postgres::reader::PgCursor;

    let (_c, pool) = start_postgres().await;
    let coord = coordinator(pool);

    let s = scope();
    let owner_a = OwnerId::new("worker-a").unwrap();
    let owner_b = OwnerId::new("worker-b").unwrap();
    let long_lease = std::time::Duration::from_secs(60);

    let lease = coord
        .claim(&s, &owner_a, partition(0), long_lease)
        .await
        .unwrap()
        .unwrap();

    coord.checkpoint(&lease, PgCursor::new(100)).await.unwrap();
    coord.checkpoint(&lease, PgCursor::new(50)).await.unwrap();

    coord.release(&lease).await.unwrap();

    let new_lease = coord
        .claim(&s, &owner_b, partition(0), long_lease)
        .await
        .unwrap()
        .expect("should claim after release");
    assert_eq!(new_lease.checkpoint_cursor.unwrap().sequence, 100);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_coordinator_checkpoint_after_release_returns_ownership_lost() {
    use eventuary_postgres::reader::PgCursor;

    let (_c, pool) = start_postgres().await;
    let coord = coordinator(pool);

    let s = scope();
    let owner_a = OwnerId::new("worker-a").unwrap();
    let lease_dur = std::time::Duration::from_secs(60);

    let lease = coord
        .claim(&s, &owner_a, partition(0), lease_dur)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(lease.generation.get(), 1);

    coord.release(&lease).await.unwrap();

    let err = coord
        .checkpoint(&lease, PgCursor::new(100))
        .await
        .unwrap_err();
    assert!(matches!(err, eventuary_core::Error::OwnershipLost(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_coordinator_checkpoint_with_stale_generation_returns_ownership_lost() {
    use eventuary_postgres::reader::PgCursor;

    let (_c, pool) = start_postgres().await;
    let coord = coordinator(pool);

    let s = scope();
    let owner_a = OwnerId::new("worker-a").unwrap();
    let owner_b = OwnerId::new("worker-b").unwrap();
    let short_lease = std::time::Duration::from_millis(50);
    let long_lease = std::time::Duration::from_secs(60);

    let lease_a = coord
        .claim(&s, &owner_a, partition(0), short_lease)
        .await
        .unwrap()
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    coord
        .claim(&s, &owner_b, partition(0), long_lease)
        .await
        .unwrap()
        .unwrap();

    let err = coord
        .checkpoint(&lease_a, PgCursor::new(100))
        .await
        .unwrap_err();
    assert!(matches!(err, eventuary_core::Error::OwnershipLost(_)));
}
