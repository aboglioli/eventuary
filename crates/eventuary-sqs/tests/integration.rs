use std::time::Duration;

use aws_config::BehaviorVersion;
use aws_sdk_sqs::Client;
use aws_sdk_sqs::config::{Credentials, Region};
use chrono::Utc;
use futures::StreamExt;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};

use eventuary::io::Writer;
use eventuary::io::acker::AckBufferConfig;
use eventuary::{Error, Event, OrganizationId, Payload, StartFrom};

use eventuary_sqs::{SqsReader, SqsReaderConfig, SqsWriter};

const LOCALSTACK_IMAGE: &str = "localstack/localstack";
const LOCALSTACK_TAG: &str = "3.8.1";

async fn start_localstack() -> (ContainerAsync<GenericImage>, Client) {
    let container = GenericImage::new(LOCALSTACK_IMAGE, LOCALSTACK_TAG)
        .with_exposed_port(4566.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Ready."))
        .start()
        .await
        .expect("start localstack");
    let port = container.get_host_port_ipv4(4566).await.unwrap();
    let endpoint = format!("http://127.0.0.1:{port}");
    let creds = Credentials::new("test", "test", None, None, "static");
    let cfg = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .credentials_provider(creds)
        .load()
        .await;
    let client = Client::new(&cfg);
    (container, client)
}

async fn create_queue(client: &Client, name: &str) -> String {
    client
        .create_queue()
        .queue_name(name)
        .send()
        .await
        .unwrap()
        .queue_url
        .unwrap()
}

async fn approximate_messages(client: &Client, queue_url: &str) -> i32 {
    let resp = client
        .get_queue_attributes()
        .queue_url(queue_url)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages)
        .send()
        .await
        .unwrap();
    resp.attributes()
        .and_then(|m| m.get(&aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages))
        .and_then(|s| s.parse::<i32>().ok())
        .unwrap_or(-1)
}

fn make_event(org: &str, key: &str) -> Event {
    Event::create(org, "/x", "thing.happened", key, Payload::from_string("v")).unwrap()
}

#[tokio::test]
async fn writer_sends_serialized_event() {
    let (_c, client) = start_localstack().await;
    let queue_url = create_queue(&client, "q-writer").await;
    let writer = SqsWriter::new(client.clone(), &queue_url);
    let event = make_event("orgsqs", "k1");
    writer.write(&event).await.unwrap();

    let resp = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(1)
        .wait_time_seconds(5)
        .send()
        .await
        .unwrap();
    let messages = resp.messages.unwrap_or_default();
    assert_eq!(messages.len(), 1);
    let body = messages[0].body.as_deref().unwrap();
    let value: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(value["organization"], "orgsqs");
    assert_eq!(value["topic"], "thing.happened");
    assert_eq!(value["key"], "k1");
    assert_eq!(value["namespace"], "/x");
}

#[tokio::test]
async fn reader_receives_event() {
    let (_c, client) = start_localstack().await;
    let queue_url = create_queue(&client, "q-reader").await;
    let writer = SqsWriter::new(client.clone(), &queue_url);
    let event = make_event("orgsqs", "k-recv");
    writer.write(&event).await.unwrap();

    let config = SqsReaderConfig::defaults_for(&queue_url, OrganizationId::new("orgsqs").unwrap());
    let reader = SqsReader::new(client.clone(), config).unwrap();
    let mut stream = reader.read().await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(30), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(msg.event().key().as_str(), "k-recv");
    assert_eq!(msg.event().topic().as_str(), "thing.happened");
}

#[tokio::test]
async fn ack_deletes_message() {
    let (_c, client) = start_localstack().await;
    let queue_url = create_queue(&client, "q-ack").await;
    let writer = SqsWriter::new(client.clone(), &queue_url);
    writer.write(&make_event("orgsqs", "k-ack")).await.unwrap();

    let mut config =
        SqsReaderConfig::defaults_for(&queue_url, OrganizationId::new("orgsqs").unwrap());
    config.ack_buffer = AckBufferConfig {
        max_pending: 1,
        flush_interval: Duration::from_millis(50),
    };
    let reader = SqsReader::new(client.clone(), config).unwrap();
    let mut stream = reader.read().await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(30), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(msg.event().key().as_str(), "k-ack");
    msg.ack().await.unwrap();
    drop(stream);

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let n = approximate_messages(&client, &queue_url).await;
        if n == 0 {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("queue still has {n} messages after ack");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
async fn org_mismatch_is_acked_and_skipped() {
    let (_c, client) = start_localstack().await;
    let queue_url = create_queue(&client, "q-org").await;
    let writer = SqsWriter::new(client.clone(), &queue_url);
    writer
        .write(&make_event("other-org", "wrong"))
        .await
        .unwrap();
    writer.write(&make_event("orgsqs", "right")).await.unwrap();

    let config = SqsReaderConfig::defaults_for(&queue_url, OrganizationId::new("orgsqs").unwrap());
    let reader = SqsReader::new(client.clone(), config).unwrap();
    let mut stream = reader.read().await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(30), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(msg.event().key().as_str(), "right");
}

#[tokio::test]
async fn invalid_start_from_is_rejected() {
    let org = OrganizationId::new("o").unwrap();
    let queue_url = "https://example.com/queue".to_owned();

    let mut config = SqsReaderConfig::defaults_for(&queue_url, org.clone());
    config.start_from = StartFrom::Earliest;
    let err = config.validate().unwrap_err();
    assert!(matches!(err, Error::Config(_)));

    let mut config = SqsReaderConfig::defaults_for(&queue_url, org);
    config.start_from = StartFrom::Timestamp(Utc::now());
    let err = config.validate().unwrap_err();
    assert!(matches!(err, Error::Config(_)));
}

#[tokio::test]
async fn invalid_wait_time_is_rejected() {
    let org = OrganizationId::new("o").unwrap();
    let mut config = SqsReaderConfig::defaults_for("https://q", org);
    config.wait_time = Duration::from_secs(21);
    let err = config.validate().unwrap_err();
    assert!(matches!(err, Error::Config(_)));
}

#[tokio::test]
async fn invalid_visibility_timeout_is_rejected() {
    let org = OrganizationId::new("o").unwrap();
    let mut config = SqsReaderConfig::defaults_for("https://q", org);
    config.visibility_timeout = Duration::from_secs(43_201);
    let err = config.validate().unwrap_err();
    assert!(matches!(err, Error::Config(_)));
}
