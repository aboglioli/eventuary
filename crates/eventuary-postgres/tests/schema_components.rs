use sqlx::{PgPool, Row};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

use eventuary_postgres::checkpoint::{PgCheckpointStore, PgCheckpointStoreConfig};
use eventuary_postgres::database::PgDatabase;
use eventuary_postgres::dedupe::{PgDedupeStore, PgDedupeStoreConfig};
use eventuary_postgres::reader::PgCursor;
use eventuary_postgres::writer::{PgWriter, PgWriterConfig};

async fn start_postgres() -> (ContainerAsync<GenericImage>, PgPool, String) {
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
    let pool = PgPool::connect(&url).await.unwrap();
    (container, pool, url)
}

async fn table_names(pool: &PgPool) -> Vec<String> {
    let rows = sqlx::query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    let mut tables: Vec<String> = rows.into_iter().map(|row| row.get("table_name")).collect();
    tables.sort();
    tables
}

#[tokio::test]
async fn pg_database_connect_creates_no_component_tables() {
    let (_container, pool, url) = start_postgres().await;
    drop(pool);

    let db = PgDatabase::connect(&url).await.unwrap();

    assert_eq!(table_names(&db.pool()).await, Vec::<String>::new());
}

#[tokio::test]
async fn pg_dedupe_connect_creates_only_dedupe_table() {
    let (_container, pool, _url) = start_postgres().await;

    PgDedupeStore::connect(pool.clone(), PgDedupeStoreConfig::default())
        .await
        .unwrap();

    assert_eq!(table_names(&pool).await, vec!["dedupe_keys".to_owned()]);
}

#[tokio::test]
async fn pg_writer_connect_creates_only_events_table() {
    let (_container, pool, _url) = start_postgres().await;

    PgWriter::connect(pool.clone(), PgWriterConfig::default())
        .await
        .unwrap();

    assert_eq!(table_names(&pool).await, vec!["events".to_owned()]);
}

#[tokio::test]
async fn pg_writer_and_checkpoint_connect_create_only_their_tables() {
    let (_container, pool, _url) = start_postgres().await;

    PgWriter::connect(pool.clone(), PgWriterConfig::default())
        .await
        .unwrap();
    PgCheckpointStore::<PgCursor>::connect(pool.clone(), PgCheckpointStoreConfig::default())
        .await
        .unwrap();

    assert_eq!(
        table_names(&pool).await,
        vec!["consumer_offsets".to_owned(), "events".to_owned()]
    );
}

async fn column_names(pool: &PgPool) -> Vec<String> {
    let rows = sqlx::query(
        "SELECT column_name FROM information_schema.columns \
         WHERE table_schema = 'public' AND table_name = 'events'",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    rows.into_iter().map(|row| row.get("column_name")).collect()
}

#[tokio::test]
async fn pg_writer_connect_twice_keeps_the_split_versions() {
    let (_container, pool, _url) = start_postgres().await;

    PgWriter::connect(pool.clone(), PgWriterConfig::default())
        .await
        .unwrap();
    PgWriter::connect(pool.clone(), PgWriterConfig::default())
        .await
        .unwrap();

    let columns = column_names(&pool).await;
    assert!(columns.contains(&"schema_version".to_owned()));
    assert!(columns.contains(&"spec_version".to_owned()));
    assert!(!columns.contains(&"version".to_owned()));
}

#[tokio::test]
async fn pg_writer_connect_upgrades_a_log_with_the_single_version_column() {
    let (_container, pool, _url) = start_postgres().await;
    sqlx::raw_sql(
        "CREATE TABLE events (
            sequence BIGSERIAL PRIMARY KEY,
            id UUID NOT NULL UNIQUE,
            organization TEXT NOT NULL,
            namespace TEXT NOT NULL,
            topic TEXT NOT NULL,
            event_key TEXT NOT NULL,
            payload JSONB NOT NULL,
            content_type TEXT NOT NULL,
            metadata JSONB NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            version BIGINT NOT NULL,
            partition_key TEXT,
            partition_hash BIGINT,
            partition_id BIGINT,
            partition_count BIGINT,
            partition_strategy TEXT
        );
        INSERT INTO events (id, organization, namespace, topic, event_key, payload,
            content_type, metadata, timestamp, version)
        VALUES (gen_random_uuid(), 'acme', '/x', 'thing.happened', 'k', '{}',
            'application/json', '{}', now(), 4);",
    )
    .execute(&pool)
    .await
    .unwrap();

    PgWriter::connect(pool.clone(), PgWriterConfig::default())
        .await
        .unwrap();

    let row = sqlx::query("SELECT schema_version, spec_version FROM events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row.get::<i64, _>("schema_version"), 4);
    assert_eq!(row.get::<i32, _>("spec_version"), 1);
}
