use std::time::Duration;

use eventuary_core::io::Writer;
use eventuary_core::{Error, Event, Payload};
use eventuary_sqlite::database::SqliteDatabase;
use eventuary_sqlite::writer::{SqliteWriter, SqliteWriterConfig};

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
async fn a_write_blocked_by_another_connection_is_reported_as_contention() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.db");

    let owner = SqliteDatabase::open(&path).unwrap();
    SqliteWriter::prepare_schema(&owner.conn(), &SqliteWriterConfig::default()).unwrap();

    let contender = SqliteDatabase::open(&path).unwrap();
    contender
        .conn()
        .lock()
        .unwrap()
        .busy_timeout(Duration::from_millis(50))
        .unwrap();
    let writer = SqliteWriter::new(contender.conn());

    let held = owner.conn();
    held.lock()
        .unwrap()
        .execute_batch("BEGIN EXCLUSIVE")
        .unwrap();

    let started = std::time::Instant::now();
    let refused = writer
        .write(&ev("order-1"))
        .await
        .expect_err("the database is held by another connection");
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "the busy timeout bounds the wait, as lock_wait does for eventuary-fs"
    );
    assert!(
        matches!(refused, Error::Contended(_)),
        "a busy database must be retryable, not an opaque store failure: {refused:?}"
    );

    held.lock().unwrap().execute_batch("ROLLBACK").unwrap();

    writer
        .write(&ev("order-1"))
        .await
        .expect("the same write succeeds once the database is free");
}
