use sqlx::PgPool;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

use eventuary_core::{Event, Payload};
use eventuary_postgres::database::PgDatabase;
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
    sqlx::query("CREATE TABLE orders (id TEXT PRIMARY KEY)")
        .execute(&pool)
        .await
        .unwrap();
    (container, pool)
}

fn order_placed(key: &str) -> Event {
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

async fn events_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM events")
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn orders_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM orders")
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_event_written_in_a_committed_transaction_is_persisted_with_the_state_change() {
    let (_c, pool) = start_postgres().await;
    let writer = PgWriter::new(pool.clone());

    let mut tx = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO orders (id) VALUES ('order-1')")
        .execute(&mut *tx)
        .await
        .unwrap();
    writer
        .write_with(&mut *tx, &order_placed("order-1"))
        .await
        .unwrap();
    tx.commit().await.unwrap();

    assert_eq!(orders_count(&pool).await, 1);
    assert_eq!(events_count(&pool).await, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_event_written_in_a_rolled_back_transaction_is_discarded_with_the_state_change() {
    let (_c, pool) = start_postgres().await;
    let writer = PgWriter::new(pool.clone());

    let mut tx = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO orders (id) VALUES ('order-1')")
        .execute(&mut *tx)
        .await
        .unwrap();
    writer
        .write_with(&mut *tx, &order_placed("order-1"))
        .await
        .unwrap();
    tx.rollback().await.unwrap();

    assert_eq!(orders_count(&pool).await, 0);
    assert_eq!(events_count(&pool).await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_event_written_in_a_transaction_is_invisible_to_other_connections_until_commit() {
    let (_c, pool) = start_postgres().await;
    let writer = PgWriter::new(pool.clone());

    let mut tx = pool.begin().await.unwrap();
    writer
        .write_with(&mut *tx, &order_placed("order-1"))
        .await
        .unwrap();

    assert_eq!(events_count(&pool).await, 0);
    tx.commit().await.unwrap();
    assert_eq!(events_count(&pool).await, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn events_written_together_in_a_committed_transaction_are_all_persisted() {
    let (_c, pool) = start_postgres().await;
    let writer = PgWriter::new(pool.clone());
    let events = [order_placed("order-1"), order_placed("order-2")];

    let mut tx = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO orders (id) VALUES ('order-1'), ('order-2')")
        .execute(&mut *tx)
        .await
        .unwrap();
    writer.write_all_with(&mut *tx, &events).await.unwrap();
    tx.commit().await.unwrap();

    assert_eq!(orders_count(&pool).await, 2);
    assert_eq!(events_count(&pool).await, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn events_written_together_in_a_rolled_back_transaction_are_all_discarded() {
    let (_c, pool) = start_postgres().await;
    let writer = PgWriter::new(pool.clone());
    let events = [order_placed("order-1"), order_placed("order-2")];

    let mut tx = pool.begin().await.unwrap();
    writer.write_all_with(&mut *tx, &events).await.unwrap();
    tx.rollback().await.unwrap();

    assert_eq!(events_count(&pool).await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_event_written_with_the_pool_is_persisted_at_once() {
    let (_c, pool) = start_postgres().await;
    let writer = PgWriter::new(pool.clone());

    writer
        .write_with(&pool, &order_placed("order-1"))
        .await
        .unwrap();

    assert_eq!(events_count(&pool).await, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn events_written_together_with_the_pool_land_all_or_none() {
    let (_c, pool) = start_postgres().await;
    let writer = PgWriter::new(pool.clone());
    let event = order_placed("order-1");

    let result = writer
        .write_all_with(&pool, &[order_placed("order-2"), event.clone(), event])
        .await;

    assert!(result.is_err());
    assert_eq!(events_count(&pool).await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_batch_inside_a_transaction_leaves_the_transaction_usable() {
    let (_c, pool) = start_postgres().await;
    let writer = PgWriter::new(pool.clone());
    let event = order_placed("order-1");

    let mut tx = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO orders (id) VALUES ('order-1')")
        .execute(&mut *tx)
        .await
        .unwrap();
    let result = writer
        .write_all_with(&mut *tx, &[event.clone(), event])
        .await;
    tx.commit().await.unwrap();

    assert!(result.is_err());
    assert_eq!(orders_count(&pool).await, 1);
    assert_eq!(events_count(&pool).await, 0);
}
