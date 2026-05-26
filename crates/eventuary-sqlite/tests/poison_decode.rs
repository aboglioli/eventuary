use std::time::Duration;

use futures::StreamExt;
use tokio::time::timeout;

use eventuary_core::io::reader::ReaderTypedExt;
use eventuary_core::io::{Reader, Writer};
use eventuary_core::{Event, JsonPayloadCodec, Payload, StartFrom};
use eventuary_sqlite::database::{SqliteConn, SqliteDatabase};
use eventuary_sqlite::reader::{SqliteReader, SqliteReaderConfig, SqliteSubscription};
use eventuary_sqlite::writer::{SqliteWriter, SqliteWriterConfig};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct OrderPlaced {
    order_id: String,
    amount: u64,
}

fn prepare_test_schema(conn: &SqliteConn) {
    SqliteWriter::prepare_schema(conn, &SqliteWriterConfig::default()).unwrap();
}

#[tokio::test]
async fn decode_reader_skips_poison_payload_and_continues() {
    let db = SqliteDatabase::open_in_memory().unwrap();
    prepare_test_schema(&db.conn());
    let writer = SqliteWriter::new(db.conn());

    let valid = Event::builder(
        "acme",
        "/orders",
        "order.placed",
        "order-1",
        Payload::from_json(&serde_json::json!({"order_id": "o-1", "amount": 42})).unwrap(),
    )
    .unwrap()
    .build()
    .unwrap();
    writer.write(&valid).await.unwrap();

    let poison = Event::builder(
        "acme",
        "/orders",
        "order.placed",
        "order-poison",
        Payload::from_json(&serde_json::json!({"garbage": true})).unwrap(),
    )
    .unwrap()
    .build()
    .unwrap();
    writer.write(&poison).await.unwrap();

    let valid2 = Event::builder(
        "acme",
        "/orders",
        "order.placed",
        "order-2",
        Payload::from_json(&serde_json::json!({"order_id": "o-2", "amount": 99})).unwrap(),
    )
    .unwrap()
    .build()
    .unwrap();
    writer.write(&valid2).await.unwrap();

    let source = SqliteReader::new(
        db.conn(),
        SqliteReaderConfig {
            poll_interval: Duration::from_millis(10),
            ..SqliteReaderConfig::default()
        },
    );
    let typed_reader = source.decode::<OrderPlaced, _>(JsonPayloadCodec);

    let sub = SqliteSubscription {
        start: StartFrom::Earliest,
        ..SqliteSubscription::default()
    };

    let mut stream = typed_reader.read(sub).await.unwrap();

    let msg1 = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(msg1.event().payload().order_id, "o-1");
    msg1.ack().await.unwrap();

    // Poison decode error surfaces on stream (AckInner advances source cursor)
    let poison_err = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap();
    assert!(poison_err.is_err(), "poison event should surface as decode error");

    let msg2 = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(msg2.event().payload().order_id, "o-2");
    msg2.ack().await.unwrap();
}
