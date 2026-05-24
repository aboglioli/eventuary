use std::collections::HashSet;
use std::time::Duration;

use sqlx::PgPool;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

use eventuary_core::io::OwnerId;
use eventuary_core::io::reader::claim_buffer::ClaimedBufferStore;
use eventuary_core::{Event, Payload};
use eventuary_postgres::database::PgDatabase;
use eventuary_postgres::{PgClaimedBufferStore, PgClaimedBufferStoreConfig};

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

fn ev(key: &str) -> Event {
    Event::builder("org", "/x", "thing.happened", Payload::from_string("p"))
        .unwrap()
        .key(key)
        .unwrap()
        .build()
        .unwrap()
}

fn owner(s: &str) -> OwnerId {
    OwnerId::new(s).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_then_claim_returns_entry() {
    let (_c, pool) = start_postgres().await;
    let store = PgClaimedBufferStore::new(pool, PgClaimedBufferStoreConfig::default());
    store.push(&ev("a")).await.unwrap();
    let batch = store
        .claim_batch(&owner("owner-a"), 10, Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].attempts, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claim_returns_at_most_max() {
    let (_c, pool) = start_postgres().await;
    let store = PgClaimedBufferStore::new(pool, PgClaimedBufferStoreConfig::default());
    for i in 0..5 {
        store.push(&ev(&format!("k{i}"))).await.unwrap();
    }
    let first = store
        .claim_batch(&owner("owner-a"), 3, Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(first.len(), 3);

    let second = store
        .claim_batch(&owner("owner-b"), 10, Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(second.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claim_skips_currently_claimed() {
    let (_c, pool) = start_postgres().await;
    let store = PgClaimedBufferStore::new(pool, PgClaimedBufferStoreConfig::default());
    for i in 0..3 {
        store.push(&ev(&format!("k{i}"))).await.unwrap();
    }
    let first = store
        .claim_batch(&owner("owner-a"), 10, Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(first.len(), 3);

    let second = store
        .claim_batch(&owner("owner-b"), 10, Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(second.len(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claim_returns_expired_entries() {
    let (_c, pool) = start_postgres().await;
    let store = PgClaimedBufferStore::new(pool, PgClaimedBufferStoreConfig::default());
    store.push(&ev("a")).await.unwrap();
    store
        .claim_batch(&owner("owner-a"), 10, Duration::from_millis(50))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    let second = store
        .claim_batch(&owner("owner-b"), 10, Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].attempts, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ack_removes_entry() {
    let (_c, pool) = start_postgres().await;
    let store = PgClaimedBufferStore::new(pool, PgClaimedBufferStoreConfig::default());
    store.push(&ev("a")).await.unwrap();
    let batch = store
        .claim_batch(&owner("owner-a"), 10, Duration::from_secs(60))
        .await
        .unwrap();
    store.ack(&batch[0].id).await.unwrap();

    let after = store
        .claim_batch(&owner("owner-b"), 10, Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(after.len(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nack_releases_immediately() {
    let (_c, pool) = start_postgres().await;
    let store = PgClaimedBufferStore::new(pool, PgClaimedBufferStoreConfig::default());
    store.push(&ev("a")).await.unwrap();
    let batch = store
        .claim_batch(&owner("owner-a"), 10, Duration::from_secs(60))
        .await
        .unwrap();
    store.nack(&batch[0].id).await.unwrap();

    let second = store
        .claim_batch(&owner("owner-b"), 10, Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].attempts, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_claim_returns_disjoint_sets() {
    let (_c, pool) = start_postgres().await;
    let store = PgClaimedBufferStore::new(pool, PgClaimedBufferStoreConfig::default());

    for i in 0..10 {
        store.push(&ev(&format!("k{i}"))).await.unwrap();
    }

    let store_a = store.clone();
    let store_b = store.clone();

    let handle_a = tokio::spawn(async move {
        store_a
            .claim_batch(&owner("owner-a"), 5, Duration::from_secs(60))
            .await
            .unwrap()
    });
    let handle_b = tokio::spawn(async move {
        store_b
            .claim_batch(&owner("owner-b"), 5, Duration::from_secs(60))
            .await
            .unwrap()
    });

    let batch_a = handle_a.await.unwrap();
    let batch_b = handle_b.await.unwrap();

    let ids_a: HashSet<i64> = batch_a.iter().map(|e| e.id).collect();
    let ids_b: HashSet<i64> = batch_b.iter().map(|e| e.id).collect();

    assert_eq!(ids_a.len() + ids_b.len(), 10);
    assert!(ids_a.is_disjoint(&ids_b));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ack_missing_id_is_noop() {
    let (_c, pool) = start_postgres().await;
    let store = PgClaimedBufferStore::new(pool, PgClaimedBufferStoreConfig::default());
    let result = store.ack(&9999_i64).await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nack_missing_id_is_noop() {
    let (_c, pool) = start_postgres().await;
    let store = PgClaimedBufferStore::new(pool, PgClaimedBufferStoreConfig::default());
    let result = store.nack(&9999_i64).await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fifo_order_preserved() {
    let (_c, pool) = start_postgres().await;
    let store = PgClaimedBufferStore::new(pool, PgClaimedBufferStoreConfig::default());

    let id1 = store.push(&ev("first")).await.unwrap();
    let id2 = store.push(&ev("second")).await.unwrap();
    let id3 = store.push(&ev("third")).await.unwrap();

    let batch = store
        .claim_batch(&owner("owner-a"), 10, Duration::from_secs(60))
        .await
        .unwrap();

    assert_eq!(batch.len(), 3);
    assert_eq!(batch[0].id, id1);
    assert_eq!(batch[1].id, id2);
    assert_eq!(batch[2].id, id3);
}
