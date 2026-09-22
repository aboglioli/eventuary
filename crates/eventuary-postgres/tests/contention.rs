use std::time::Duration;

use sqlx::{Executor, PgPool};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

use eventuary_core::io::Writer;
use eventuary_core::{Error, Event, Payload};
use eventuary_postgres::database::PgDatabase;
use eventuary_postgres::writer::{PgWriter, PgWriterConfig};

async fn start_postgres() -> (ContainerAsync<GenericImage>, PgPool, PgPool) {
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
    let base = format!("postgres://eventuary:eventuary@127.0.0.1:{port}/eventuary");

    let plain = PgDatabase::connect(&base).await.unwrap().pool();
    let impatient = PgDatabase::connect(&format!("{base}?options=-c%20lock_timeout%3D200ms"))
        .await
        .unwrap()
        .pool();
    (container, plain, impatient)
}

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

#[tokio::test]
async fn a_write_blocked_by_another_transaction_is_reported_as_contention() {
    let (_container, plain, impatient) = start_postgres().await;

    let writer = PgWriter::connect(impatient, PgWriterConfig::default())
        .await
        .unwrap();
    writer.write(&ev("order-1")).await.unwrap();

    let mut holder = plain.acquire().await.unwrap();
    holder.execute("BEGIN").await.unwrap();
    holder
        .execute("LOCK TABLE events IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();

    let started = std::time::Instant::now();
    let refused = writer
        .write(&ev("order-2"))
        .await
        .expect_err("the events table is locked by another transaction");
    assert!(
        matches!(refused, Error::Contended(_)),
        "a lock timeout must be retryable, not an opaque store failure: {refused:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "lock_timeout bounds the wait"
    );

    holder.execute("ROLLBACK").await.unwrap();
    drop(holder);

    writer
        .write(&ev("order-3"))
        .await
        .expect("the same write succeeds once the table is free");
}
