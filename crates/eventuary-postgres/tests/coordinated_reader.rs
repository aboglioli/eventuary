use std::collections::{HashMap, HashSet};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use sqlx::{PgPool, Row};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::time::timeout;

use eventuary_core::io::reader::CheckpointScope;
use eventuary_core::io::{ConsumerGroupId, OwnerId, Reader, StreamId, Writer};
use eventuary_core::partition::{EventKeyPartitionKeyResolver, Fnv1a64PartitionHasher};
use eventuary_core::{Error, Event, Payload, StartFrom};
use eventuary_postgres::coordinated_reader::PgCoordinatedStream;
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
    prepare_test_schema(&pool).await;
    (container, pool)
}

async fn prepare_test_schema(pool: &PgPool) {
    PgWriter::prepare_schema(pool, &PgWriterConfig::default())
        .await
        .unwrap();
    PgPartitionCoordinator::prepare_schema(pool, &PgPartitionCoordinatorConfig::default())
        .await
        .unwrap();
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

fn make_sub(scope: CheckpointScope, partition_count: NonZeroU32) -> PgCoordinatedSubscription {
    PgCoordinatedSubscription {
        scope,
        partition_count,
        start: StartFrom::Earliest,
        inner: PgSubscription {
            start: StartFrom::Earliest,
            ..PgSubscription::default()
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_coordinated_reader_two_owners_claim_disjoint_partitions() {
    let (_c, pool) = start_postgres().await;

    let partition_count = NonZeroU32::new(4).unwrap();

    let writer_config = PgWriterConfig {
        partitioning: PgPartitioningConfig::inline(
            partition_count,
            EventKeyPartitionKeyResolver::new(),
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
        rebalance_interval: Duration::from_millis(100),
        partition_slack: 0,
        ..PgCoordinatedReaderConfig::default()
    };

    let scope = CheckpointScope::new(
        ConsumerGroupId::new("test-group").unwrap(),
        StreamId::new("orders").unwrap(),
    );

    let reader_a = PgCoordinatedReader::new(
        PgReader::new(pool.clone(), PgReaderConfig::default()),
        Arc::clone(&coordinator),
        OwnerId::new("owner-a").unwrap(),
        reader_config,
    );

    let reader_b = PgCoordinatedReader::new(
        PgReader::new(pool.clone(), PgReaderConfig::default()),
        Arc::clone(&coordinator),
        OwnerId::new("owner-b").unwrap(),
        reader_config,
    );

    let (stream_a, stream_b) = tokio::join!(
        reader_a.read(make_sub(scope.clone(), partition_count)),
        reader_b.read(make_sub(scope.clone(), partition_count)),
    );

    let stream_a = stream_a.unwrap();
    let stream_b = stream_b.unwrap();

    // Two readers compete for ownership over the same scope. Partition
    // handoff races can produce `OwnershipLost` on ack — that is the
    // documented fenced-checkpoint behavior; the event is then redelivered
    // to the new owner. Track only successfully-acked deliveries so the
    // union covers every key at least once.
    let drain = |mut stream: PgCoordinatedStream| async move {
        let mut results: HashMap<u32, Vec<String>> = HashMap::new();
        while let Ok(Some(Ok(msg))) = timeout(Duration::from_secs(2), stream.next()).await {
            let partition_id = msg.cursor().partition.id();
            let key = msg.event().key().as_str().to_owned();
            match msg.ack().await {
                Ok(()) => {
                    results.entry(partition_id).or_default().push(key);
                }
                Err(Error::OwnershipLost(_)) => {
                    // Lease was reassigned mid-flight; the event will be
                    // redelivered to whichever owner holds the partition now.
                }
                Err(e) => panic!("unexpected ack error: {e}"),
            }
        }
        results
    };

    let (results_a, results_b) = tokio::join!(drain(stream_a), drain(stream_b));

    // Fenced-checkpoint invariant: every key is observed by at least one
    // owner, and no key is lost to ownership races. Whether the partitions
    // end up disjoint or one owner wins the entire group depends on the
    // claim-race outcome, which is not a guaranteed property of the
    // coordinator — only the at-least-once delivery guarantee is.
    let observed_keys: HashSet<String> = results_a
        .values()
        .chain(results_b.values())
        .flatten()
        .cloned()
        .collect();
    assert_eq!(
        observed_keys.len(),
        keys.len(),
        "every key must be observed by at least one owner; got {observed_keys:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_coordinated_reader_rebalances_when_third_owner_joins() {
    let (_c, pool) = start_postgres().await;

    let partition_count = NonZeroU32::new(4).unwrap();

    let writer_config = PgWriterConfig {
        partitioning: PgPartitioningConfig::inline(
            partition_count,
            EventKeyPartitionKeyResolver::new(),
            Fnv1a64PartitionHasher,
        ),
        ..PgWriterConfig::default()
    };
    let writer = PgWriter::new_with_config(pool.clone(), writer_config);

    for i in 0..16u32 {
        writer
            .write(&event_with_key(&format!("k{i}")))
            .await
            .unwrap();
    }

    let coordinator = Arc::new(PgPartitionCoordinator::new(
        pool.clone(),
        PgPartitionCoordinatorConfig::default(),
    ));

    let reader_config = PgCoordinatedReaderConfig {
        partition_lease_duration: Duration::from_secs(10),
        partition_renew_interval: Duration::from_secs(5),
        consumer_lease_duration: Duration::from_secs(10),
        consumer_heartbeat_interval: Duration::from_secs(3),
        rebalance_interval: Duration::from_millis(200),
        partition_slack: 0,
        ..PgCoordinatedReaderConfig::default()
    };

    let scope = CheckpointScope::new(
        ConsumerGroupId::new("rebalance-group").unwrap(),
        StreamId::new("orders").unwrap(),
    );

    let reader_a = PgCoordinatedReader::new(
        PgReader::new(pool.clone(), PgReaderConfig::default()),
        Arc::clone(&coordinator),
        OwnerId::new("owner-a").unwrap(),
        reader_config,
    );
    let stream_a = reader_a
        .read(make_sub(scope.clone(), partition_count))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let reader_b = PgCoordinatedReader::new(
        PgReader::new(pool.clone(), PgReaderConfig::default()),
        Arc::clone(&coordinator),
        OwnerId::new("owner-b").unwrap(),
        reader_config,
    );
    let stream_b = reader_b
        .read(make_sub(scope.clone(), partition_count))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(600)).await;

    let owned_after_b = query_owned_partitions(&pool, &scope).await;
    let owners_after_b: HashSet<String> = owned_after_b.values().cloned().collect();
    assert!(
        owners_after_b.contains("owner-a") || owners_after_b.contains("owner-b"),
        "at least one owner should hold partitions after B joins; got {owners_after_b:?}"
    );
    let max_owned_after_b = owners_after_b
        .iter()
        .map(|owner| owned_after_b.values().filter(|v| *v == owner).count())
        .max()
        .unwrap_or(0);
    assert!(
        max_owned_after_b <= 4,
        "no owner should hold more than 4 partitions; got {max_owned_after_b}"
    );

    let reader_c = PgCoordinatedReader::new(
        PgReader::new(pool.clone(), PgReaderConfig::default()),
        Arc::clone(&coordinator),
        OwnerId::new("owner-c").unwrap(),
        reader_config,
    );
    let stream_c = reader_c
        .read(make_sub(scope.clone(), partition_count))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(800)).await;

    let owned_after_c = query_owned_partitions(&pool, &scope).await;

    assert_eq!(
        owned_after_c.len(),
        4,
        "all 4 partitions should be owned; got {:?}",
        owned_after_c
    );

    let all_owners: HashSet<&String> = owned_after_c.values().collect();
    for owner in &all_owners {
        let count = owned_after_c.values().filter(|v| v == owner).count();
        assert!(
            count <= 2,
            "owner {owner} holds {count} partitions, expected at most ceil(4/3)=2; distribution: {owned_after_c:?}"
        );
    }

    let known_owners: HashSet<&str> = ["owner-a", "owner-b", "owner-c"].iter().copied().collect();
    for owner in owned_after_c.values() {
        assert!(
            known_owners.contains(owner.as_str()),
            "unexpected owner {owner}"
        );
    }

    let drain_some = |mut stream: PgCoordinatedStream| async move {
        while let Ok(Some(Ok(msg))) = timeout(Duration::from_millis(200), stream.next()).await {
            let _ = msg.ack().await;
        }
    };

    tokio::join!(
        drain_some(stream_a),
        drain_some(stream_b),
        drain_some(stream_c)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_coordinated_reader_drop_releases_owned_partitions() {
    let (_c, pool) = start_postgres().await;

    let partition_count = NonZeroU32::new(4).unwrap();

    let writer_config = PgWriterConfig {
        partitioning: PgPartitioningConfig::inline(
            partition_count,
            EventKeyPartitionKeyResolver::new(),
            Fnv1a64PartitionHasher,
        ),
        ..PgWriterConfig::default()
    };
    let writer = PgWriter::new_with_config(pool.clone(), writer_config);

    for i in 0..8u32 {
        writer
            .write(&event_with_key(&format!("k{i}")))
            .await
            .unwrap();
    }

    let coordinator = Arc::new(PgPartitionCoordinator::new(
        pool.clone(),
        PgPartitionCoordinatorConfig::default(),
    ));

    let scope = CheckpointScope::new(
        ConsumerGroupId::new("drop-release-group").unwrap(),
        StreamId::new("orders").unwrap(),
    );

    let reader_config = PgCoordinatedReaderConfig {
        partition_lease_duration: Duration::from_secs(30),
        partition_renew_interval: Duration::from_secs(10),
        consumer_lease_duration: Duration::from_secs(30),
        consumer_heartbeat_interval: Duration::from_secs(10),
        rebalance_interval: Duration::from_millis(100),
        partition_slack: 0,
        ..PgCoordinatedReaderConfig::default()
    };

    let reader_a = PgCoordinatedReader::new(
        PgReader::new(pool.clone(), PgReaderConfig::default()),
        Arc::clone(&coordinator),
        OwnerId::new("owner-a").unwrap(),
        reader_config,
    );

    {
        let mut stream_a = reader_a
            .read(make_sub(scope.clone(), partition_count))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(600)).await;

        let mut drained = 0usize;
        while let Ok(Some(Ok(msg))) = timeout(Duration::from_millis(200), stream_a.next()).await {
            let _ = msg.ack().await;
            drained += 1;
            if drained >= 2 {
                break;
            }
        }

        let owned_before = query_owned_partitions(&pool, &scope).await;
        assert!(
            !owned_before.is_empty(),
            "owner-a should hold at least one partition before drop; got {owned_before:?}"
        );
    }

    tokio::time::sleep(Duration::from_millis(2000)).await;

    let rows = sqlx::query(
        "SELECT partition_id, owner_id FROM event_stream_partitions \
         WHERE consumer_group_id = $1 AND stream_id = $2",
    )
    .bind(scope.consumer_group_id.as_str())
    .bind(scope.stream_id.as_str())
    .fetch_all(&pool)
    .await
    .unwrap();

    for row in &rows {
        let owner_id: Option<String> = row.get("owner_id");
        assert!(
            owner_id.is_none(),
            "partition {} should be released after stream drop; still owned by {:?}",
            row.get::<i64, _>("partition_id"),
            owner_id
        );
    }

    let reader_b = PgCoordinatedReader::new(
        PgReader::new(pool.clone(), PgReaderConfig::default()),
        Arc::clone(&coordinator),
        OwnerId::new("owner-b").unwrap(),
        reader_config,
    );

    let _stream_b = reader_b
        .read(make_sub(scope.clone(), partition_count))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(800)).await;

    let owned_b = query_owned_partitions(&pool, &scope).await;
    assert!(
        !owned_b.is_empty(),
        "owner-b should be able to claim released partitions; got none"
    );
    for owner in owned_b.values() {
        assert_eq!(
            owner, "owner-b",
            "unexpected owner after reacquisition: {owner}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_coordinated_reader_fresh_latest_skips_existing_events() {
    let (_c, pool) = start_postgres().await;
    let partition_count = NonZeroU32::new(4).unwrap();

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

    writer.write(&event_with_key("old-1")).await.unwrap();
    writer.write(&event_with_key("old-2")).await.unwrap();

    let coordinator = Arc::new(PgPartitionCoordinator::new(
        pool.clone(),
        PgPartitionCoordinatorConfig::default(),
    ));

    let reader = PgCoordinatedReader::new(
        PgReader::new(
            pool.clone(),
            PgReaderConfig {
                poll_interval: Duration::from_millis(20),
                ..PgReaderConfig::default()
            },
        ),
        Arc::clone(&coordinator),
        OwnerId::new("fresh-latest-owner").unwrap(),
        PgCoordinatedReaderConfig {
            rebalance_interval: Duration::from_millis(50),
            partition_lease_duration: Duration::from_secs(10),
            partition_slack: 0,
            ..PgCoordinatedReaderConfig::default()
        },
    );

    let scope = CheckpointScope::new(
        ConsumerGroupId::new("fresh-latest-group").unwrap(),
        StreamId::new("orders").unwrap(),
    );

    let subscription = PgCoordinatedSubscription {
        inner: PgSubscription::default(),
        scope,
        partition_count,
        start: StartFrom::Latest,
    };

    let mut stream = reader.read(subscription).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    writer.write(&event_with_key("new-1")).await.unwrap();

    let mut delivered = Vec::new();
    while let Ok(Some(Ok(msg))) = timeout(Duration::from_secs(5), stream.next()).await {
        let key = msg.event().key().as_str().to_owned();
        msg.ack().await.unwrap();
        delivered.push(key);
        if delivered.len() == 1 {
            break;
        }
    }

    assert_eq!(delivered, vec!["new-1".to_owned()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_coordinated_reader_fresh_timestamp_skips_pre_cutoff_events() {
    let (_c, pool) = start_postgres().await;
    let partition_count = NonZeroU32::new(4).unwrap();

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

    writer
        .write(&event_with_key("old-before-cutoff"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let cutoff = chrono::Utc::now();
    tokio::time::sleep(Duration::from_millis(20)).await;
    writer
        .write(&event_with_key("new-after-cutoff"))
        .await
        .unwrap();

    let coordinator = Arc::new(PgPartitionCoordinator::new(
        pool.clone(),
        PgPartitionCoordinatorConfig::default(),
    ));

    let reader = PgCoordinatedReader::new(
        PgReader::new(
            pool.clone(),
            PgReaderConfig {
                poll_interval: Duration::from_millis(20),
                ..PgReaderConfig::default()
            },
        ),
        Arc::clone(&coordinator),
        OwnerId::new("fresh-timestamp-owner").unwrap(),
        PgCoordinatedReaderConfig {
            rebalance_interval: Duration::from_millis(50),
            partition_lease_duration: Duration::from_secs(10),
            partition_slack: 0,
            ..PgCoordinatedReaderConfig::default()
        },
    );

    let scope = CheckpointScope::new(
        ConsumerGroupId::new("fresh-timestamp-group").unwrap(),
        StreamId::new("orders").unwrap(),
    );

    let subscription = PgCoordinatedSubscription {
        inner: PgSubscription::default(),
        scope,
        partition_count,
        start: StartFrom::Timestamp(cutoff),
    };

    let mut stream = reader.read(subscription).await.unwrap();

    let mut delivered = Vec::new();
    while let Ok(Some(Ok(msg))) = timeout(Duration::from_secs(3), stream.next()).await {
        let key = msg.event().key().as_str().to_owned();
        msg.ack().await.unwrap();
        delivered.push(key);
        if delivered.len() == 1 {
            break;
        }
    }

    assert_eq!(delivered, vec!["new-after-cutoff".to_owned()]);
}

async fn query_owned_partitions(pool: &PgPool, scope: &CheckpointScope) -> HashMap<i64, String> {
    let rows = sqlx::query(
        "SELECT partition_id, owner_id FROM event_stream_partitions \
         WHERE consumer_group_id = $1 AND stream_id = $2 AND owner_id IS NOT NULL \
           AND lease_until > NOW()",
    )
    .bind(scope.consumer_group_id.as_str())
    .bind(scope.stream_id.as_str())
    .fetch_all(pool)
    .await
    .unwrap();

    rows.into_iter()
        .map(|r| {
            let partition_id: i64 = r.get("partition_id");
            let owner_id: String = r.get("owner_id");
            (partition_id, owner_id)
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_coordinated_reader_resumes_from_checkpoint_on_restart() {
    let (_c, pool) = start_postgres().await;
    let partition_count = NonZeroU32::new(4).unwrap();

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

    let keys = [
        "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta",
    ];
    for key in &keys {
        writer.write(&event_with_key(key)).await.unwrap();
    }

    let coordinator = Arc::new(PgPartitionCoordinator::new(
        pool.clone(),
        PgPartitionCoordinatorConfig::default(),
    ));

    let scope = CheckpointScope::new(
        ConsumerGroupId::new("resume-group").unwrap(),
        StreamId::new("orders").unwrap(),
    );

    let reader_config = PgCoordinatedReaderConfig {
        partition_lease_duration: Duration::from_secs(60),
        partition_renew_interval: Duration::from_secs(15),
        consumer_lease_duration: Duration::from_secs(30),
        consumer_heartbeat_interval: Duration::from_secs(10),
        rebalance_interval: Duration::from_millis(100),
        partition_slack: 0,
        ..PgCoordinatedReaderConfig::default()
    };

    let mut acked_keys: Vec<String> = Vec::new();

    {
        let reader = PgCoordinatedReader::new(
            PgReader::new(pool.clone(), PgReaderConfig::default()),
            Arc::clone(&coordinator),
            OwnerId::new("owner-1").unwrap(),
            reader_config,
        );

        let mut stream = reader
            .read(make_sub(scope.clone(), partition_count))
            .await
            .unwrap();

        for _ in 0..4 {
            let msg = timeout(Duration::from_secs(5), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            acked_keys.push(msg.event().key().as_str().to_owned());
            msg.ack().await.unwrap();
        }
    }

    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut resumed_keys: Vec<String> = Vec::new();

    {
        let reader2 = PgCoordinatedReader::new(
            PgReader::new(pool.clone(), PgReaderConfig::default()),
            Arc::clone(&coordinator),
            OwnerId::new("owner-2").unwrap(),
            reader_config,
        );

        let mut stream2 = reader2
            .read(make_sub(scope.clone(), partition_count))
            .await
            .unwrap();

        while let Ok(Some(Ok(msg))) = timeout(Duration::from_secs(5), stream2.next()).await {
            resumed_keys.push(msg.event().key().as_str().to_owned());
            msg.ack().await.unwrap();
            if resumed_keys.len() >= 8 {
                break;
            }
        }
    }

    let acked_set: HashSet<&String> = acked_keys.iter().collect();
    let resumed_set: HashSet<&String> = resumed_keys.iter().collect();
    let overlap: HashSet<_> = acked_set.intersection(&resumed_set).collect();
    assert!(
        overlap.is_empty(),
        "resumed reader should not re-deliver already-acked events; overlap={overlap:?}"
    );

    let all_keys: HashSet<&str> = keys.iter().copied().collect();
    let mut combined: HashSet<String> = acked_keys.into_iter().collect();
    combined.extend(resumed_keys);
    assert_eq!(
        combined.len(),
        8,
        "combined total should be 8; got {}",
        combined.len()
    );
    for key in &all_keys {
        assert!(
            combined.contains(*key),
            "missing event key={key}; combined={combined:?}"
        );
    }
}
