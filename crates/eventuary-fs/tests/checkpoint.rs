use std::num::NonZeroU32;
use std::time::Duration;

use futures::StreamExt;
use tokio::time::timeout;

use eventuary_core::io::reader::{
    CheckpointKey, CheckpointReader, CheckpointScope, CheckpointStore, CheckpointSubscription,
};
use eventuary_core::io::{ConsumerGroupId, CursorId, Reader, StreamId, Writer};
use eventuary_core::partition::{EventKeyPartitionKeyResolver, Fnv1a64PartitionHasher, Partition};
use eventuary_core::{Event, Payload, StopAt};
use eventuary_fs::checkpoint::{FsCheckpointStore, FsCheckpointStoreConfig};
use eventuary_fs::reader::{FsCursor, FsReader, FsReaderConfig, FsSubscription};
use eventuary_fs::writer::{FsPartitioningConfig, FsWriter, FsWriterConfig};

fn ev(key: &str) -> Event {
    Event::builder(
        "acme",
        "/orders",
        "order.created",
        key,
        Payload::from_string("p"),
    )
    .unwrap()
    .build()
    .unwrap()
}

fn partitioned(count: u32) -> FsWriterConfig {
    FsWriterConfig {
        partitioning: FsPartitioningConfig::inline(
            NonZeroU32::new(count).unwrap(),
            EventKeyPartitionKeyResolver::new(),
            Fnv1a64PartitionHasher,
        ),
        ..FsWriterConfig::default()
    }
}

fn fast() -> FsReaderConfig {
    FsReaderConfig {
        poll_interval: Duration::from_millis(5),
        ..FsReaderConfig::default()
    }
}

fn scope() -> CheckpointScope {
    CheckpointScope::new(
        ConsumerGroupId::new("workers").unwrap(),
        StreamId::new("orders").unwrap(),
    )
}

fn global_key() -> CheckpointKey {
    CheckpointKey::new(scope(), CursorId::global())
}

fn cursor(offset: u64) -> FsCursor {
    FsCursor::new(partition(0, 1), offset)
}

fn partition(id: u32, count: u32) -> Partition {
    Partition::new(id, NonZeroU32::new(count).unwrap()).unwrap()
}

#[tokio::test]
async fn commit_then_load_round_trips_a_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let store: FsCheckpointStore<FsCursor> =
        FsCheckpointStore::open(dir.path(), FsCheckpointStoreConfig::default()).unwrap();
    let cursor = FsCursor::new(partition(3, 8), 42);
    let key = CheckpointKey::new(scope(), CursorId::partition(partition(3, 8)));

    store.commit(&key, cursor).await.unwrap();
    let loaded = store.load(&key).await.unwrap();

    assert_eq!(loaded, Some(cursor));
}

#[tokio::test]
async fn load_returns_none_for_an_unknown_key() {
    let dir = tempfile::tempdir().unwrap();
    let store: FsCheckpointStore<FsCursor> =
        FsCheckpointStore::open(dir.path(), FsCheckpointStoreConfig::default()).unwrap();
    let key = CheckpointKey::new(scope(), CursorId::global());

    assert_eq!(store.load(&key).await.unwrap(), None);
}

#[tokio::test]
async fn commit_overwrites_a_previous_value() {
    let dir = tempfile::tempdir().unwrap();
    let store: FsCheckpointStore<FsCursor> =
        FsCheckpointStore::open(dir.path(), FsCheckpointStoreConfig::default()).unwrap();
    let key = CheckpointKey::new(scope(), CursorId::partition(partition(0, 4)));

    store
        .commit(&key, FsCursor::new(partition(0, 4), 1))
        .await
        .unwrap();
    store
        .commit(&key, FsCursor::new(partition(0, 4), 9))
        .await
        .unwrap();

    assert_eq!(store.load(&key).await.unwrap().unwrap().offset, 9);
}

#[tokio::test]
async fn load_scope_returns_every_cursor_in_the_scope() {
    let dir = tempfile::tempdir().unwrap();
    let store: FsCheckpointStore<FsCursor> =
        FsCheckpointStore::open(dir.path(), FsCheckpointStoreConfig::default()).unwrap();
    for id in 0..4 {
        let p = partition(id, 4);
        store
            .commit(
                &CheckpointKey::new(scope(), CursorId::partition(p)),
                FsCursor::new(p, u64::from(id) * 10),
            )
            .await
            .unwrap();
    }

    let loaded = store.load_scope(&scope()).await.unwrap();

    assert_eq!(loaded.len(), 4);
    assert!(
        loaded
            .iter()
            .all(|(id, c)| *id == CursorId::partition(c.partition))
    );
}

#[tokio::test]
async fn scopes_are_isolated_from_each_other() {
    let dir = tempfile::tempdir().unwrap();
    let store: FsCheckpointStore<FsCursor> =
        FsCheckpointStore::open(dir.path(), FsCheckpointStoreConfig::default()).unwrap();
    let other = CheckpointScope::new(
        ConsumerGroupId::new("other").unwrap(),
        StreamId::new("orders").unwrap(),
    );
    store
        .commit(
            &CheckpointKey::new(scope(), CursorId::global()),
            FsCursor::new(partition(0, 1), 5),
        )
        .await
        .unwrap();

    assert!(store.load_scope(&other).await.unwrap().is_empty());
    assert_eq!(store.load_scope(&scope()).await.unwrap().len(), 1);
}

#[tokio::test]
async fn checkpoints_survive_reopening_the_store() {
    let dir = tempfile::tempdir().unwrap();
    let key = CheckpointKey::new(scope(), CursorId::partition(partition(1, 2)));
    {
        let store: FsCheckpointStore<FsCursor> =
            FsCheckpointStore::open(dir.path(), FsCheckpointStoreConfig::default()).unwrap();
        store
            .commit(&key, FsCursor::new(partition(1, 2), 77))
            .await
            .unwrap();
    }

    let store: FsCheckpointStore<FsCursor> =
        FsCheckpointStore::open(dir.path(), FsCheckpointStoreConfig::default()).unwrap();

    assert_eq!(store.load(&key).await.unwrap().unwrap().offset, 77);
}

#[tokio::test]
async fn checkpoint_reader_resumes_where_the_previous_run_stopped() {
    let dir = tempfile::tempdir().unwrap();
    let writer = FsWriter::open(dir.path(), FsWriterConfig::default()).unwrap();
    for i in 0..6 {
        writer.write(&ev(&format!("k{i}"))).await.unwrap();
    }
    writer.sync().await.unwrap();

    let store: FsCheckpointStore<FsCursor> =
        FsCheckpointStore::open(dir.path(), FsCheckpointStoreConfig::default()).unwrap();

    let first: Vec<String> = {
        let reader =
            CheckpointReader::new(FsReader::open(dir.path(), fast()).unwrap(), store.clone());
        let mut stream = reader
            .read(CheckpointSubscription::new(
                FsSubscription {
                    limit: Some(3),
                    ..FsSubscription::earliest()
                },
                scope(),
            ))
            .await
            .unwrap();
        let mut keys = Vec::new();
        while keys.len() < 3 {
            let message = timeout(Duration::from_secs(5), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            message.ack().await.unwrap();
            keys.push(message.event().key().as_str().to_owned());
        }
        stream.flush().await.unwrap();
        keys
    };

    let reader = CheckpointReader::new(FsReader::open(dir.path(), fast()).unwrap(), store);
    let mut stream = reader
        .read(CheckpointSubscription::new(
            FsSubscription {
                stop_at: StopAt::CurrentEnd,
                ..FsSubscription::earliest()
            },
            scope(),
        ))
        .await
        .unwrap();
    let mut second = Vec::new();
    while let Ok(Some(item)) = timeout(Duration::from_secs(5), stream.next()).await {
        let message = item.unwrap();
        message.ack().await.unwrap();
        second.push(message.event().key().as_str().to_owned());
    }

    assert_eq!(first, vec!["k0", "k1", "k2"]);
    assert_eq!(second, vec!["k3", "k4", "k5"]);
}

#[tokio::test]
async fn checkpoint_resume_is_independent_per_partition() {
    let dir = tempfile::tempdir().unwrap();
    let writer = FsWriter::open(dir.path(), partitioned(4)).unwrap();
    for i in 0..32 {
        writer.write(&ev(&format!("k{i}"))).await.unwrap();
    }
    writer.sync().await.unwrap();

    let store: FsCheckpointStore<FsCursor> =
        FsCheckpointStore::open(dir.path(), FsCheckpointStoreConfig::default()).unwrap();
    let reader = CheckpointReader::new(FsReader::open(dir.path(), fast()).unwrap(), store.clone());
    let mut stream = reader
        .read(CheckpointSubscription::new(
            FsSubscription {
                limit: Some(10),
                ..FsSubscription::earliest()
            },
            scope(),
        ))
        .await
        .unwrap();
    let mut seen = Vec::new();
    while seen.len() < 10 {
        let message = timeout(Duration::from_secs(5), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        message.ack().await.unwrap();
        seen.push(message.event().key().as_str().to_owned());
    }
    stream.flush().await.unwrap();
    drop(stream);

    let committed = store.load_scope(&scope()).await.unwrap();
    assert!(!committed.is_empty());

    let reader = CheckpointReader::new(FsReader::open(dir.path(), fast()).unwrap(), store);
    let mut stream = reader
        .read(CheckpointSubscription::new(
            FsSubscription {
                stop_at: StopAt::CurrentEnd,
                ..FsSubscription::earliest()
            },
            scope(),
        ))
        .await
        .unwrap();
    let mut rest = Vec::new();
    while let Ok(Some(item)) = timeout(Duration::from_secs(5), stream.next()).await {
        let message = item.unwrap();
        message.ack().await.unwrap();
        rest.push(message.event().key().as_str().to_owned());
    }

    let mut all: Vec<String> = seen.into_iter().chain(rest).collect();
    all.sort();
    all.dedup();
    assert_eq!(all.len(), 32);
}

#[tokio::test]
async fn concurrent_commits_to_one_key_never_publish_a_torn_file() {
    let dir = tempfile::tempdir().unwrap();
    let key = CheckpointKey::new(
        CheckpointScope::new(
            ConsumerGroupId::new("group").unwrap(),
            StreamId::new("stream").unwrap(),
        ),
        CursorId::global(),
    );

    let mut committers = Vec::new();
    for n in 0..8u64 {
        let store: FsCheckpointStore<FsCursor> =
            FsCheckpointStore::open(dir.path(), FsCheckpointStoreConfig::default()).unwrap();
        let key = key.clone();
        committers.push(tokio::spawn(async move {
            for round in 0..25 {
                let offset = n * 100 + round;
                store
                    .commit(
                        &key,
                        FsCursor::new(
                            Partition::new(0, NonZeroU32::new(1).unwrap()).unwrap(),
                            offset,
                        ),
                    )
                    .await
                    .unwrap();
            }
        }));
    }
    for task in committers {
        task.await.unwrap();
    }

    let store: FsCheckpointStore<FsCursor> =
        FsCheckpointStore::open(dir.path(), FsCheckpointStoreConfig::default()).unwrap();
    let loaded = store
        .load(&key)
        .await
        .expect("a committed checkpoint must always decode");
    assert!(
        loaded.is_some(),
        "the last rename must publish a whole cursor"
    );
}

#[tokio::test]
async fn a_checkpoint_only_moves_forward() {
    let dir = tempfile::tempdir().unwrap();
    let store: FsCheckpointStore<FsCursor> =
        FsCheckpointStore::open(dir.path(), FsCheckpointStoreConfig::default()).unwrap();
    let key = global_key();

    store.commit(&key, cursor(100)).await.unwrap();
    store.commit(&key, cursor(50)).await.unwrap();

    assert_eq!(
        store.load(&key).await.unwrap().unwrap().offset(),
        100,
        "a stale commit must not rewind the checkpoint, as in the SQL stores"
    );

    store.commit(&key, cursor(200)).await.unwrap();
    assert_eq!(store.load(&key).await.unwrap().unwrap().offset(), 200);
}

#[tokio::test]
async fn concurrent_commits_settle_on_the_highest_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let key = global_key();

    let mut committers = Vec::new();
    for offset in [70u64, 10, 90, 30, 50] {
        let root = dir.path().to_path_buf();
        let key = key.clone();
        committers.push(tokio::spawn(async move {
            let store: FsCheckpointStore<FsCursor> =
                FsCheckpointStore::open(&root, FsCheckpointStoreConfig::default()).unwrap();
            store.commit(&key, cursor(offset)).await.unwrap();
        }));
    }
    for task in committers {
        task.await.unwrap();
    }

    let store: FsCheckpointStore<FsCursor> =
        FsCheckpointStore::open(dir.path(), FsCheckpointStoreConfig::default()).unwrap();
    assert_eq!(store.load(&key).await.unwrap().unwrap().offset(), 90);
}
