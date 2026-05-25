use std::sync::{Arc, Mutex};

use rusqlite::Connection;

use eventuary_sqlite::checkpoint_store::{SqliteCheckpointStore, SqliteCheckpointStoreConfig};
use eventuary_sqlite::database::{SqliteConn, SqliteDatabase};
use eventuary_sqlite::dedupe_store::{SqliteDedupeStore, SqliteDedupeStoreConfig};
use eventuary_sqlite::reader::SqliteCursor;
use eventuary_sqlite::writer::{SqliteWriter, SqliteWriterConfig};

fn sqlite_master_names(conn: &Connection, object_type: &str) -> Vec<String> {
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = ?1 ORDER BY name")
        .unwrap();
    stmt.query_map([object_type], |row| row.get::<_, String>(0))
        .unwrap()
        .map(Result::unwrap)
        .filter(|name| !name.starts_with("sqlite_"))
        .collect()
}

#[test]
fn sqlite_database_open_creates_no_component_tables() {
    let db = SqliteDatabase::open_in_memory().unwrap();
    let conn = db.conn();
    let guard = conn.lock().unwrap();
    assert_eq!(sqlite_master_names(&guard, "table"), Vec::<String>::new());
}

#[test]
fn sqlite_dedupe_connect_creates_only_dedupe_table() {
    let conn: SqliteConn = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));

    SqliteDedupeStore::connect(Arc::clone(&conn), SqliteDedupeStoreConfig::default()).unwrap();

    let guard = conn.lock().unwrap();
    assert_eq!(
        sqlite_master_names(&guard, "table"),
        vec!["dedupe_keys".to_owned()]
    );
}

#[test]
fn sqlite_writer_connect_creates_only_event_log_table() {
    let conn: SqliteConn = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));

    SqliteWriter::connect(Arc::clone(&conn), SqliteWriterConfig::default()).unwrap();

    let guard = conn.lock().unwrap();
    assert_eq!(
        sqlite_master_names(&guard, "table"),
        vec!["events".to_owned()]
    );
}

#[test]
fn sqlite_writer_and_checkpoint_connect_create_only_their_tables() {
    let conn: SqliteConn = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));

    SqliteWriter::connect(Arc::clone(&conn), SqliteWriterConfig::default()).unwrap();
    SqliteCheckpointStore::<SqliteCursor>::connect(
        Arc::clone(&conn),
        SqliteCheckpointStoreConfig::default(),
    )
    .unwrap();

    let guard = conn.lock().unwrap();
    assert_eq!(
        sqlite_master_names(&guard, "table"),
        vec!["consumer_offsets".to_owned(), "events".to_owned()]
    );
}
