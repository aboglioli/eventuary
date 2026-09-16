use std::num::NonZeroU32;
use std::time::Duration;

use eventuary_core::io::reader::{CheckpointScope, PartitionCoordinator, PartitionLease};
use eventuary_core::io::{ConsumerGroupId, OwnerId, StreamId};
use eventuary_core::{Error, Partition};
use eventuary_fs::coordinator::{FsPartitionCoordinator, FsPartitionCoordinatorConfig};
use eventuary_fs::reader::FsCursor;

const COUNT: u32 = 4;

fn scope() -> CheckpointScope {
    CheckpointScope::new(
        ConsumerGroupId::new("workers").unwrap(),
        StreamId::new("orders").unwrap(),
    )
}

fn owner(name: &str) -> OwnerId {
    OwnerId::new(name).unwrap()
}

fn partition(id: u32) -> Partition {
    Partition::new(id, NonZeroU32::new(COUNT).unwrap()).unwrap()
}

fn cursor(offset: u64) -> FsCursor {
    FsCursor::new(partition(0), offset)
}

fn coordinator(dir: &tempfile::TempDir) -> FsPartitionCoordinator<FsCursor> {
    FsPartitionCoordinator::open(dir.path(), FsPartitionCoordinatorConfig::default()).unwrap()
}

#[tokio::test]
async fn first_claim_succeeds_and_starts_at_generation_one() {
    let dir = tempfile::tempdir().unwrap();
    let c = coordinator(&dir);

    let lease = c
        .claim(&scope(), &owner("a"), partition(0), Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(lease.generation.get(), 1);
    assert_eq!(lease.owner_id.as_str(), "a");
    assert!(lease.checkpoint_cursor.is_none());
}

#[tokio::test]
async fn a_second_owner_cannot_claim_a_live_partition() {
    let dir = tempfile::tempdir().unwrap();
    let c = coordinator(&dir);
    c.claim(&scope(), &owner("a"), partition(0), Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();

    let taken = c
        .claim(&scope(), &owner("b"), partition(0), Duration::from_secs(30))
        .await
        .unwrap();

    assert!(taken.is_none());
}

#[tokio::test]
async fn the_same_owner_reclaiming_bumps_the_generation() {
    let dir = tempfile::tempdir().unwrap();
    let c = coordinator(&dir);
    let first = c
        .claim(&scope(), &owner("a"), partition(0), Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();

    let second = c
        .claim(&scope(), &owner("a"), partition(0), Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(second.generation.get(), first.generation.get() + 1);
}

#[tokio::test]
async fn an_expired_lease_can_be_taken_by_another_owner() {
    let dir = tempfile::tempdir().unwrap();
    let c = coordinator(&dir);
    c.claim(
        &scope(),
        &owner("a"),
        partition(0),
        Duration::from_millis(1),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    let taken = c
        .claim(&scope(), &owner("b"), partition(0), Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(taken.owner_id.as_str(), "b");
    assert_eq!(taken.generation.get(), 2);
}

#[tokio::test]
async fn distinct_partitions_are_claimed_independently() {
    let dir = tempfile::tempdir().unwrap();
    let c = coordinator(&dir);

    let a = c
        .claim(&scope(), &owner("a"), partition(0), Duration::from_secs(30))
        .await
        .unwrap();
    let b = c
        .claim(&scope(), &owner("b"), partition(1), Duration::from_secs(30))
        .await
        .unwrap();

    assert!(a.is_some());
    assert!(b.is_some());
}

#[tokio::test]
async fn concurrent_claims_of_one_partition_produce_a_single_winner() {
    let dir = tempfile::tempdir().unwrap();
    let c = coordinator(&dir);

    let mut handles = Vec::new();
    for i in 0..16 {
        let c = c.clone();
        handles.push(tokio::spawn(async move {
            c.claim(
                &scope(),
                &owner(&format!("owner-{i}")),
                partition(0),
                Duration::from_secs(30),
            )
            .await
            .unwrap()
        }));
    }
    let mut winners = Vec::new();
    for handle in handles {
        if let Some(lease) = handle.await.unwrap() {
            winners.push(lease);
        }
    }

    assert_eq!(winners.len(), 1);
    assert_eq!(winners[0].generation.get(), 1);
}

#[tokio::test]
async fn renew_extends_a_held_lease() {
    let dir = tempfile::tempdir().unwrap();
    let c = coordinator(&dir);
    let lease = c
        .claim(
            &scope(),
            &owner("a"),
            partition(0),
            Duration::from_millis(50),
        )
        .await
        .unwrap()
        .unwrap();

    c.renew(&lease, Duration::from_secs(60)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;

    let stolen = c
        .claim(&scope(), &owner("b"), partition(0), Duration::from_secs(30))
        .await
        .unwrap();
    assert!(stolen.is_none());
}

#[tokio::test]
async fn renew_after_a_takeover_reports_ownership_lost() {
    let dir = tempfile::tempdir().unwrap();
    let c = coordinator(&dir);
    let stale = c
        .claim(
            &scope(),
            &owner("a"),
            partition(0),
            Duration::from_millis(1),
        )
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    c.claim(&scope(), &owner("b"), partition(0), Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();

    let result = c.renew(&stale, Duration::from_secs(30)).await;

    assert!(matches!(result, Err(Error::OwnershipLost(_))));
}

#[tokio::test]
async fn release_frees_the_partition_and_bumps_the_generation() {
    let dir = tempfile::tempdir().unwrap();
    let c = coordinator(&dir);
    let lease = c
        .claim(&scope(), &owner("a"), partition(0), Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();

    c.release(&lease).await.unwrap();
    let taken = c
        .claim(&scope(), &owner("b"), partition(0), Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(taken.owner_id.as_str(), "b");
    assert_eq!(taken.generation.get(), 3);
}

#[tokio::test]
async fn release_with_a_stale_generation_reports_ownership_lost() {
    let dir = tempfile::tempdir().unwrap();
    let c = coordinator(&dir);
    let stale = c
        .claim(&scope(), &owner("a"), partition(0), Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    c.claim(&scope(), &owner("a"), partition(0), Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();

    let result = c.release(&stale).await;

    assert!(matches!(result, Err(Error::OwnershipLost(_))));
}

#[tokio::test]
async fn checkpoint_is_visible_to_the_next_claim() {
    let dir = tempfile::tempdir().unwrap();
    let c = coordinator(&dir);
    let lease = c
        .claim(&scope(), &owner("a"), partition(0), Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();

    c.checkpoint(&lease, cursor(41)).await.unwrap();
    c.release(&lease).await.unwrap();
    let resumed = c
        .claim(&scope(), &owner("b"), partition(0), Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(resumed.checkpoint_cursor.map(|c| c.offset), Some(41));
}

#[tokio::test]
async fn checkpoint_is_monotonic_and_ignores_regressions() {
    let dir = tempfile::tempdir().unwrap();
    let c = coordinator(&dir);
    let lease = c
        .claim(&scope(), &owner("a"), partition(0), Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();

    c.checkpoint(&lease, cursor(10)).await.unwrap();
    c.checkpoint(&lease, cursor(4)).await.unwrap();
    c.release(&lease).await.unwrap();
    let resumed = c
        .claim(&scope(), &owner("b"), partition(0), Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(resumed.checkpoint_cursor.map(|c| c.offset), Some(10));
}

#[tokio::test]
async fn checkpoint_after_losing_ownership_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let c = coordinator(&dir);
    let stale = c
        .claim(
            &scope(),
            &owner("a"),
            partition(0),
            Duration::from_millis(1),
        )
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    c.claim(&scope(), &owner("b"), partition(0), Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();

    let result = c.checkpoint(&stale, cursor(99)).await;

    assert!(matches!(result, Err(Error::OwnershipLost(_))));
}

#[tokio::test]
async fn claiming_with_a_different_partition_count_is_a_config_error() {
    let dir = tempfile::tempdir().unwrap();
    let c = coordinator(&dir);
    c.claim(&scope(), &owner("a"), partition(0), Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let other = Partition::new(0, NonZeroU32::new(8).unwrap()).unwrap();

    let result = c
        .claim(&scope(), &owner("b"), other, Duration::from_secs(30))
        .await;

    assert!(matches!(result, Err(Error::Config(_))));
}

#[tokio::test]
async fn heartbeat_registers_a_live_consumer() {
    let dir = tempfile::tempdir().unwrap();
    let c = coordinator(&dir);

    assert_eq!(c.live_consumers(&scope()).await.unwrap(), 0);
    c.heartbeat(&scope(), &owner("a"), Duration::from_secs(30))
        .await
        .unwrap();

    assert_eq!(c.live_consumers(&scope()).await.unwrap(), 1);
}

#[tokio::test]
async fn live_consumers_counts_distinct_owners_only() {
    let dir = tempfile::tempdir().unwrap();
    let c = coordinator(&dir);

    for _ in 0..3 {
        c.heartbeat(&scope(), &owner("a"), Duration::from_secs(30))
            .await
            .unwrap();
    }
    c.heartbeat(&scope(), &owner("b"), Duration::from_secs(30))
        .await
        .unwrap();

    assert_eq!(c.live_consumers(&scope()).await.unwrap(), 2);
}

#[tokio::test]
async fn expired_consumers_are_not_counted_as_live() {
    let dir = tempfile::tempdir().unwrap();
    let c = coordinator(&dir);
    c.heartbeat(&scope(), &owner("a"), Duration::from_millis(1))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    assert_eq!(c.live_consumers(&scope()).await.unwrap(), 0);
}

#[tokio::test]
async fn release_consumer_is_idempotent_and_drops_the_registration() {
    let dir = tempfile::tempdir().unwrap();
    let c = coordinator(&dir);
    c.heartbeat(&scope(), &owner("a"), Duration::from_secs(30))
        .await
        .unwrap();

    c.release_consumer(&scope(), &owner("a")).await.unwrap();
    c.release_consumer(&scope(), &owner("a")).await.unwrap();

    assert_eq!(c.live_consumers(&scope()).await.unwrap(), 0);
}

#[tokio::test]
async fn scopes_do_not_share_consumers_or_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let c = coordinator(&dir);
    let other = CheckpointScope::new(
        ConsumerGroupId::new("other").unwrap(),
        StreamId::new("orders").unwrap(),
    );
    c.heartbeat(&scope(), &owner("a"), Duration::from_secs(30))
        .await
        .unwrap();
    c.claim(&scope(), &owner("a"), partition(0), Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(c.live_consumers(&other).await.unwrap(), 0);
    let claimed_elsewhere = c
        .claim(&other, &owner("b"), partition(0), Duration::from_secs(30))
        .await
        .unwrap();
    assert!(claimed_elsewhere.is_some());
}

#[tokio::test]
async fn coordinator_state_survives_reopening() {
    let dir = tempfile::tempdir().unwrap();
    let lease: PartitionLease<FsCursor> = {
        let c = coordinator(&dir);
        let lease = c
            .claim(&scope(), &owner("a"), partition(0), Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
        c.checkpoint(&lease, cursor(7)).await.unwrap();
        lease
    };

    let c = coordinator(&dir);
    let blocked = c
        .claim(&scope(), &owner("b"), partition(0), Duration::from_secs(30))
        .await
        .unwrap();
    c.release(&lease).await.unwrap();
    let resumed = c
        .claim(&scope(), &owner("b"), partition(0), Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();

    assert!(blocked.is_none());
    assert_eq!(resumed.checkpoint_cursor.map(|c| c.offset), Some(7));
}
