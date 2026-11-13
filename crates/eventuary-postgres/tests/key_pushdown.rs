use std::time::Duration;

use futures::StreamExt;
use sqlx::{PgPool, Row};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::time::timeout;

use eventuary_core::io::filter::EventFilter;
use eventuary_core::io::{Acker, Reader, Writer};
use eventuary_core::{Event, EventKey, Metadata, Payload, StartFrom, StopAt};
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

fn event(key: &str, tenant: &str) -> Event {
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
    .with_metadata(Metadata::new().with("tenant", tenant).unwrap())
}

async fn read_keys(pool: &PgPool, filter: EventFilter, batch_size: usize) -> Vec<String> {
    let reader = PgReader::new(pool.clone(), PgReaderConfig::default());
    let mut stream = reader
        .read(PgSubscription {
            start: StartFrom::Earliest,
            stop_at: StopAt::CurrentEnd,
            filter,
            batch_size: Some(batch_size),
            ..PgSubscription::default()
        })
        .await
        .unwrap();
    let mut keys = Vec::new();
    while let Ok(Some(item)) = timeout(Duration::from_secs(5), stream.next()).await {
        let msg = item.expect("keyed read must not fail on rows outside the filter");
        keys.push(format!(
            "{}:{}",
            msg.event().key().as_str(),
            msg.event().metadata().get("tenant").unwrap()
        ));
        msg.acker().ack().await.unwrap();
    }
    keys
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keyed_read_returns_only_matching_keys_in_order() {
    let (_c, pool) = start_postgres().await;
    let writer = PgWriter::new(pool.clone());
    for (key, tenant) in [
        ("a", "t1"),
        ("b", "t1"),
        ("a", "t2"),
        ("c", "t1"),
        ("b", "t2"),
    ] {
        writer.write(&event(key, tenant)).await.unwrap();
    }

    let filter = EventFilter {
        keys: Some(vec![
            EventKey::new("a").unwrap(),
            EventKey::new("c").unwrap(),
        ]),
        ..EventFilter::default()
    };

    assert_eq!(read_keys(&pool, filter, 1).await, ["a:t1", "a:t2", "c:t1"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_filter_returns_only_matching_events() {
    let (_c, pool) = start_postgres().await;
    let writer = PgWriter::new(pool.clone());
    for (key, tenant) in [("a", "t1"), ("b", "t2"), ("c", "t1")] {
        writer.write(&event(key, tenant)).await.unwrap();
    }

    let filter = EventFilter {
        metadata: Some(Metadata::new().with("tenant", "t1").unwrap()),
        ..EventFilter::default()
    };

    assert_eq!(read_keys(&pool, filter, 10).await, ["a:t1", "c:t1"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_filter_matches_events_with_additional_entries() {
    let (_c, pool) = start_postgres().await;
    let writer = PgWriter::new(pool.clone());
    let rich = event("a", "t1").with_metadata(
        Metadata::new()
            .with("tenant", "t1")
            .unwrap()
            .with("actor_id", "u1")
            .unwrap()
            .with("region", "eu")
            .unwrap(),
    );
    writer.write(&rich).await.unwrap();
    writer.write(&event("b", "t1")).await.unwrap();

    let by_tenant = EventFilter {
        metadata: Some(Metadata::new().with("tenant", "t1").unwrap()),
        ..EventFilter::default()
    };
    let by_tenant_and_actor = EventFilter {
        metadata: Some(
            Metadata::new()
                .with("tenant", "t1")
                .unwrap()
                .with("actor_id", "u1")
                .unwrap(),
        ),
        ..EventFilter::default()
    };

    assert_eq!(read_keys(&pool, by_tenant, 10).await, ["a:t1", "b:t1"]);
    assert_eq!(read_keys(&pool, by_tenant_and_actor, 10).await, ["a:t1"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readers_without_keys_still_read_everything() {
    let (_c, pool) = start_postgres().await;
    let writer = PgWriter::new(pool.clone());
    for (key, tenant) in [("a", "t1"), ("b", "t2")] {
        writer.write(&event(key, tenant)).await.unwrap();
    }

    assert_eq!(
        read_keys(&pool, EventFilter::default(), 10).await,
        ["a:t1", "b:t2"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keyed_read_does_not_decode_rows_of_other_keys() {
    let (_c, pool) = start_postgres().await;
    let writer = PgWriter::new(pool.clone());
    writer.write(&event("a", "t1")).await.unwrap();
    writer.write(&event("b", "t1")).await.unwrap();
    sqlx::query("UPDATE events SET metadata = '[]'::jsonb WHERE event_key = 'b'")
        .execute(&pool)
        .await
        .unwrap();

    let filter = EventFilter {
        keys: Some(vec![EventKey::new("a").unwrap()]),
        ..EventFilter::default()
    };

    assert_eq!(read_keys(&pool, filter, 10).await, ["a:t1"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn event_log_indexes_organization_namespace_key_and_sequence() {
    let (_c, pool) = start_postgres().await;
    PgWriter::prepare_schema(&pool, &PgWriterConfig::default())
        .await
        .unwrap();

    let row = sqlx::query(
        "SELECT indexdef FROM pg_indexes WHERE tablename = 'events' \
         AND indexname = 'idx_events_org_namespace_key_sequence'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let definition: String = row.get("indexdef");
    assert!(definition.contains("(organization, namespace, event_key, sequence)"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_read_does_not_decode_rows_of_other_metadata() {
    let (_c, pool) = start_postgres().await;
    let writer = PgWriter::new(pool.clone());
    writer.write(&event("a", "t1")).await.unwrap();
    writer.write(&event("b", "t2")).await.unwrap();
    sqlx::query("UPDATE events SET payload = '\"corrupt\"'::jsonb WHERE event_key = 'b'")
        .execute(&pool)
        .await
        .unwrap();

    let filter = EventFilter {
        metadata: Some(Metadata::new().with("tenant", "t1").unwrap()),
        ..EventFilter::default()
    };

    assert_eq!(read_keys(&pool, filter, 10).await, ["a:t1"]);
}
