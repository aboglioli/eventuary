#![allow(dead_code)]

use std::time::Duration;

use aws_config::BehaviorVersion;
use aws_sdk_sqs::config::{Credentials, Region};
use aws_sdk_sqs::types::QueueAttributeName;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};

use eventuary_core::{Event, Payload, SerializedEvent};

pub(crate) const LOCALSTACK_IMAGE: &str = "localstack/localstack";
pub(crate) const LOCALSTACK_TAG: &str = "3.8.1";

pub(crate) struct Localstack {
    _container: ContainerAsync<GenericImage>,
    pub(crate) sqs: aws_sdk_sqs::Client,
    pub(crate) sns: aws_sdk_sns::Client,
}

pub(crate) async fn start_localstack() -> Localstack {
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
    Localstack {
        sqs: aws_sdk_sqs::Client::new(&cfg),
        sns: aws_sdk_sns::Client::new(&cfg),
        _container: container,
    }
}

pub(crate) async fn create_queue(client: &aws_sdk_sqs::Client, name: &str) -> String {
    client
        .create_queue()
        .queue_name(name)
        .send()
        .await
        .expect("create queue")
        .queue_url
        .expect("queue url")
}

pub(crate) async fn create_fifo_queue(client: &aws_sdk_sqs::Client, name: &str) -> String {
    client
        .create_queue()
        .queue_name(name)
        .attributes(QueueAttributeName::FifoQueue, "true")
        .send()
        .await
        .expect("create fifo queue")
        .queue_url
        .expect("queue url")
}

pub(crate) async fn queue_arn(client: &aws_sdk_sqs::Client, queue_url: &str) -> String {
    let resp = client
        .get_queue_attributes()
        .queue_url(queue_url)
        .attribute_names(QueueAttributeName::QueueArn)
        .send()
        .await
        .expect("get queue arn");
    resp.attributes()
        .and_then(|m| m.get(&QueueAttributeName::QueueArn))
        .expect("queue arn attribute")
        .clone()
}

pub(crate) async fn approximate_messages(client: &aws_sdk_sqs::Client, queue_url: &str) -> i32 {
    let resp = client
        .get_queue_attributes()
        .queue_url(queue_url)
        .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
        .send()
        .await
        .expect("get queue attributes");
    resp.attributes()
        .and_then(|m| m.get(&QueueAttributeName::ApproximateNumberOfMessages))
        .and_then(|s| s.parse::<i32>().ok())
        .unwrap_or(-1)
}

pub(crate) async fn send_raw(client: &aws_sdk_sqs::Client, queue_url: &str, body: &str) {
    client
        .send_message()
        .queue_url(queue_url)
        .message_body(body)
        .send()
        .await
        .expect("send raw message");
}

pub(crate) async fn drain_bodies(
    client: &aws_sdk_sqs::Client,
    queue_url: &str,
    expected: usize,
    timeout: Duration,
) -> Vec<String> {
    let deadline = std::time::Instant::now() + timeout;
    let mut bodies = Vec::new();
    while bodies.len() < expected && std::time::Instant::now() < deadline {
        let resp = client
            .receive_message()
            .queue_url(queue_url)
            .max_number_of_messages(10)
            .wait_time_seconds(2)
            .send()
            .await
            .expect("receive message");
        for message in resp.messages.unwrap_or_default() {
            if let Some(body) = message.body.clone() {
                bodies.push(body);
            }
            if let Some(receipt) = message.receipt_handle {
                let _ = client
                    .delete_message()
                    .queue_url(queue_url)
                    .receipt_handle(receipt)
                    .send()
                    .await;
            }
        }
    }
    bodies
}

pub(crate) async fn peek_one_body(
    client: &aws_sdk_sqs::Client,
    queue_url: &str,
    timeout: Duration,
) -> Option<String> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        let resp = client
            .receive_message()
            .queue_url(queue_url)
            .max_number_of_messages(1)
            .wait_time_seconds(2)
            .send()
            .await
            .expect("receive message");
        if let Some(message) = resp.messages.unwrap_or_default().into_iter().next()
            && let Some(body) = message.body
        {
            return Some(body);
        }
    }
    None
}

pub(crate) async fn approximate_messages_not_visible(
    client: &aws_sdk_sqs::Client,
    queue_url: &str,
) -> i32 {
    let resp = client
        .get_queue_attributes()
        .queue_url(queue_url)
        .attribute_names(QueueAttributeName::ApproximateNumberOfMessagesNotVisible)
        .send()
        .await
        .expect("get queue attributes");
    resp.attributes()
        .and_then(|m| m.get(&QueueAttributeName::ApproximateNumberOfMessagesNotVisible))
        .and_then(|s| s.parse::<i32>().ok())
        .unwrap_or(-1)
}

pub(crate) async fn total_messages(client: &aws_sdk_sqs::Client, queue_url: &str) -> i32 {
    approximate_messages(client, queue_url).await
        + approximate_messages_not_visible(client, queue_url).await
}

pub(crate) async fn wait_for_message_count(
    client: &aws_sdk_sqs::Client,
    queue_url: &str,
    expected: i32,
    timeout: Duration,
) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let last = total_messages(client, queue_url).await;
        if last == expected {
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!("queue has {last} messages (visible + in flight), expected {expected}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

pub(crate) async fn create_topic(client: &aws_sdk_sns::Client, name: &str) -> String {
    client
        .create_topic()
        .name(name)
        .send()
        .await
        .expect("create topic")
        .topic_arn
        .expect("topic arn")
}

pub(crate) async fn create_fifo_topic(client: &aws_sdk_sns::Client, name: &str) -> String {
    client
        .create_topic()
        .name(name)
        .attributes("FifoTopic", "true")
        .send()
        .await
        .expect("create fifo topic")
        .topic_arn
        .expect("topic arn")
}

pub(crate) async fn subscribe_queue(
    sns: &aws_sdk_sns::Client,
    sqs: &aws_sdk_sqs::Client,
    topic_arn: &str,
    queue_url: &str,
    raw: bool,
) -> String {
    let arn = queue_arn(sqs, queue_url).await;
    sns.subscribe()
        .topic_arn(topic_arn)
        .protocol("sqs")
        .endpoint(&arn)
        .attributes("RawMessageDelivery", if raw { "true" } else { "false" })
        .return_subscription_arn(true)
        .send()
        .await
        .expect("subscribe queue to topic")
        .subscription_arn
        .expect("subscription arn")
}

pub(crate) fn make_event(org: &str, key: &str) -> Event {
    Event::create(org, "/x", "thing.happened", key, Payload::from_string("v")).expect("valid event")
}

pub(crate) fn decode_event(body: &str) -> Event {
    SerializedEvent::from_json_str(body)
        .expect("decodable SerializedEvent")
        .to_event()
        .expect("valid event")
}
