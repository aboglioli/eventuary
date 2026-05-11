use std::net::TcpListener;
use std::time::Duration;

use futures::StreamExt;
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

use eventuary_core::io::Writer;
use eventuary_core::io::acker::AckBufferConfig;
use eventuary_core::{ConsumerGroupId, Error, Event, OrganizationId, Payload, StartFrom, Topic};

use eventuary_kafka::{KafkaReader, KafkaReaderConfig, KafkaWriter};

const KAFKA_IMAGE: &str = "confluentinc/cp-kafka";
const KAFKA_TAG: &str = "7.6.0";

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn start_kafka() -> (ContainerAsync<GenericImage>, String) {
    let host_port = free_port();
    let advertised = format!("PLAINTEXT://localhost:{host_port}");
    let container = GenericImage::new(KAFKA_IMAGE, KAFKA_TAG)
        .with_exposed_port(9092.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Kafka Server started"))
        .with_mapped_port(host_port, 9092.tcp())
        .with_env_var("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qk")
        .with_env_var("KAFKA_NODE_ID", "1")
        .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env_var(
            "KAFKA_LISTENERS",
            "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
        )
        .with_env_var("KAFKA_ADVERTISED_LISTENERS", advertised)
        .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env_var(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        )
        .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
        .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .start()
        .await
        .expect("kafka start");
    let brokers = format!("localhost:{host_port}");
    (container, brokers)
}

async fn create_topic(brokers: &str, topic: &str, partitions: i32) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .unwrap();
    admin
        .create_topics(
            &[NewTopic::new(topic, partitions, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .unwrap();
}

fn make_event(org: &str, namespace: &str, topic: &str, key: &str) -> Event {
    Event::builder(org, namespace, topic, Payload::from_string("v"))
        .unwrap()
        .key(key)
        .unwrap()
        .build()
        .expect("valid event")
}

#[tokio::test]
async fn write_read_roundtrip() {
    let (_c, brokers) = start_kafka().await;
    create_topic(&brokers, "topic-rr", 1).await;
    let writer = KafkaWriter::new(std::slice::from_ref(&brokers), "topic-rr").unwrap();
    writer
        .write(&make_event("orgk", "/x", "thing.happened", "k1"))
        .await
        .unwrap();

    let cfg = KafkaReaderConfig {
        start_from: StartFrom::Earliest,
        ..KafkaReaderConfig::streaming(
            vec![brokers.clone()],
            vec!["topic-rr".to_owned()],
            ConsumerGroupId::new("g-rr").unwrap(),
            OrganizationId::new("orgk").unwrap(),
        )
    };
    let reader = KafkaReader::new(cfg).unwrap();
    let mut stream = reader.read().await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(60), stream.next())
        .await
        .expect("stream next")
        .expect("some")
        .expect("ok");
    assert_eq!(msg.event().key().expect("event has key").as_str(), "k1");
    assert_eq!(msg.event().topic().as_str(), "thing.happened");
    msg.ack().await.unwrap();
}

#[tokio::test]
async fn ack_commits_offset() {
    let (_c, brokers) = start_kafka().await;
    create_topic(&brokers, "topic-ack", 1).await;
    let writer = KafkaWriter::new(std::slice::from_ref(&brokers), "topic-ack").unwrap();
    for i in 0..5 {
        writer
            .write(&make_event(
                "orgk",
                "/x",
                "thing.happened",
                &format!("k{i}"),
            ))
            .await
            .unwrap();
    }

    let group = ConsumerGroupId::new("g-ack").unwrap();
    let cfg = KafkaReaderConfig {
        start_from: StartFrom::Earliest,
        ack_buffer: AckBufferConfig {
            max_pending: 1,
            flush_interval: Duration::from_millis(100),
        },
        ..KafkaReaderConfig::streaming(
            vec![brokers.clone()],
            vec!["topic-ack".to_owned()],
            group.clone(),
            OrganizationId::new("orgk").unwrap(),
        )
    };
    let reader = KafkaReader::new(cfg).unwrap();
    let mut stream = reader.read().await.unwrap();
    for _ in 0..3 {
        let msg = tokio::time::timeout(Duration::from_secs(60), stream.next())
            .await
            .expect("stream next")
            .expect("some")
            .expect("ok");
        msg.ack().await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    drop(stream);
    drop(reader);

    let cfg2 = KafkaReaderConfig {
        start_from: StartFrom::Earliest,
        ..KafkaReaderConfig::streaming(
            vec![brokers.clone()],
            vec!["topic-ack".to_owned()],
            group,
            OrganizationId::new("orgk").unwrap(),
        )
    };
    let reader2 = KafkaReader::new(cfg2).unwrap();
    let mut stream2 = reader2.read().await.unwrap();

    let mut keys: Vec<String> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while keys.len() < 2 && std::time::Instant::now() < deadline {
        if let Ok(Some(Ok(msg))) =
            tokio::time::timeout(Duration::from_secs(5), stream2.next()).await
        {
            keys.push(
                msg.event()
                    .key()
                    .expect("event has key")
                    .as_str()
                    .to_owned(),
            );
            msg.ack().await.unwrap();
        }
    }
    assert!(
        keys.contains(&"k3".to_owned()) && keys.contains(&"k4".to_owned()),
        "expected to resume from k3 onward, got {keys:?}"
    );
    assert!(
        !keys.contains(&"k0".to_owned()),
        "k0 should have been committed and skipped"
    );
}

#[tokio::test]
async fn consumer_group_resume() {
    let (_c, brokers) = start_kafka().await;
    create_topic(&brokers, "topic-resume", 1).await;
    let writer = KafkaWriter::new(std::slice::from_ref(&brokers), "topic-resume").unwrap();
    for i in 0..6 {
        writer
            .write(&make_event(
                "orgk",
                "/x",
                "thing.happened",
                &format!("k{i}"),
            ))
            .await
            .unwrap();
    }

    let group = ConsumerGroupId::new("g-resume").unwrap();

    let mk_cfg = || KafkaReaderConfig {
        start_from: StartFrom::Earliest,
        ack_buffer: AckBufferConfig {
            max_pending: 1,
            flush_interval: Duration::from_millis(100),
        },
        ..KafkaReaderConfig::streaming(
            vec![brokers.clone()],
            vec!["topic-resume".to_owned()],
            group.clone(),
            OrganizationId::new("orgk").unwrap(),
        )
    };

    let reader = KafkaReader::new(mk_cfg()).unwrap();
    let mut stream = reader.read().await.unwrap();
    let mut first: Vec<String> = Vec::new();
    while first.len() < 3 {
        let msg = tokio::time::timeout(Duration::from_secs(60), stream.next())
            .await
            .expect("stream next")
            .expect("some")
            .expect("ok");
        first.push(
            msg.event()
                .key()
                .expect("event has key")
                .as_str()
                .to_owned(),
        );
        msg.ack().await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    drop(stream);
    drop(reader);

    let reader2 = KafkaReader::new(mk_cfg()).unwrap();
    let mut stream2 = reader2.read().await.unwrap();
    let mut second: Vec<String> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while second.len() < 3 && std::time::Instant::now() < deadline {
        if let Ok(Some(Ok(msg))) =
            tokio::time::timeout(Duration::from_secs(5), stream2.next()).await
        {
            second.push(
                msg.event()
                    .key()
                    .expect("event has key")
                    .as_str()
                    .to_owned(),
            );
            msg.ack().await.unwrap();
        }
    }
    for k in &first {
        assert!(
            !second.contains(k),
            "resumed reader replayed already-acked {k}"
        );
    }
    assert!(!second.is_empty(), "second consumer received nothing");
}

#[tokio::test]
async fn event_topic_filter_if_supported() {
    let (_c, brokers) = start_kafka().await;
    create_topic(&brokers, "topic-filt", 1).await;
    let writer = KafkaWriter::new(std::slice::from_ref(&brokers), "topic-filt").unwrap();
    writer
        .write(&make_event("orgk", "/x", "task.created", "want"))
        .await
        .unwrap();
    writer
        .write(&make_event("orgk", "/x", "task.deleted", "skip"))
        .await
        .unwrap();
    writer
        .write(&make_event("orgk", "/x", "task.created", "want2"))
        .await
        .unwrap();

    let cfg = KafkaReaderConfig {
        start_from: StartFrom::Earliest,
        event_topics: Some(vec![Topic::new("task.created").unwrap()]),
        ..KafkaReaderConfig::streaming(
            vec![brokers.clone()],
            vec!["topic-filt".to_owned()],
            ConsumerGroupId::new("g-filt").unwrap(),
            OrganizationId::new("orgk").unwrap(),
        )
    };
    let reader = KafkaReader::new(cfg).unwrap();
    let mut stream = reader.read().await.unwrap();
    let mut keys: Vec<String> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while keys.len() < 2 && std::time::Instant::now() < deadline {
        if let Ok(Some(Ok(msg))) = tokio::time::timeout(Duration::from_secs(5), stream.next()).await
        {
            keys.push(
                msg.event()
                    .key()
                    .expect("event has key")
                    .as_str()
                    .to_owned(),
            );
            msg.ack().await.unwrap();
        }
    }
    assert_eq!(keys, vec!["want".to_owned(), "want2".to_owned()]);
}

#[tokio::test]
async fn invalid_config_rejected() {
    let group = ConsumerGroupId::new("g").unwrap();
    let org = OrganizationId::new("o").unwrap();

    let err = KafkaReaderConfig::new(Vec::new(), vec!["t".to_owned()], group.clone(), org.clone())
        .unwrap_err();
    assert!(matches!(err, Error::Config(_)));

    let err = KafkaReaderConfig::new(
        vec!["b:9092".to_owned()],
        Vec::new(),
        group.clone(),
        org.clone(),
    )
    .unwrap_err();
    assert!(matches!(err, Error::Config(_)));

    let mut bad =
        KafkaReaderConfig::streaming(vec!["b:9092".to_owned()], vec!["t".to_owned()], group, org);
    bad.max_poll_records = 0;
    let err = bad.validate().unwrap_err();
    assert!(matches!(err, Error::Config(_)));
}
