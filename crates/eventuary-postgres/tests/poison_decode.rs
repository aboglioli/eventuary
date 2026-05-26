use std::time::Duration;

use futures::StreamExt;
use sqlx::PgPool;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::time::timeout;

use eventuary_core::io::reader::ReaderTypedExt;
use eventuary_core::io::{Reader, Writer};
use eventuary_core::{Event, JsonPayloadCodec, Payload, StartFrom};
use eventuary_postgres::database::PgDatabase;
use eventuary_postgres::reader::{PgReader, PgReaderConfig, PgSubscription};
use eventuary_postgres::writer::{PgWriter, PgWriterConfig};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct OrderPlaced {
    order_id: String,
    amount: u64,
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn decode_reader_skips_poison_payload_and_continues() {
    let (_c, pool) = start_postgres().await;
    let writer = PgWriter::new(pool.clone());

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

    let source = PgReader::new(
        pool.clone(),
        PgReaderConfig {
            poll_interval: Duration::from_millis(20),
            ..PgReaderConfig::default()
        },
    );
    let typed_reader = source.decode::<OrderPlaced, _>(JsonPayloadCodec);

    let sub = PgSubscription {
        start: StartFrom::Earliest,
        ..PgSubscription::default()
    };

    let mut stream = typed_reader.read(sub).await.unwrap();

    let msg1 = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(msg1.event().payload().order_id, "o-1");
    assert_eq!(msg1.event().payload().amount, 42);
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
    assert_eq!(msg2.event().payload().amount, 99);
    msg2.ack().await.unwrap();
}
