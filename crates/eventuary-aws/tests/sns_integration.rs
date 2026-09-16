mod common;

use std::collections::HashSet;
use std::time::Duration;

use futures::StreamExt;

use eventuary_core::io::Writer;
use eventuary_core::io::acker::AckBufferConfig;
use eventuary_core::{Error, Event, Payload, SerializedEvent};

use eventuary_aws::sns::writer::{SnsFifoConfig, SnsWriter, SnsWriterConfig};
use eventuary_aws::sqs::reader::{SqsReader, SqsReaderConfig};

use common::{
    create_fifo_queue, create_fifo_topic, create_queue, create_topic, decode_event, drain_bodies,
    make_event, peek_one_body, start_localstack, subscribe_queue,
};

#[tokio::test]
async fn publish_delivers_serialized_event_to_subscribed_queue() {
    let stack = start_localstack().await;
    let topic_arn = create_topic(&stack.sns, "t-publish").await;
    let queue_url = create_queue(&stack.sqs, "q-publish").await;
    subscribe_queue(&stack.sns, &stack.sqs, &topic_arn, &queue_url, true).await;

    let writer = SnsWriter::new(stack.sns.clone(), &topic_arn);
    assert_eq!(writer.topic_arn(), topic_arn);

    let event = make_event("orgsns", "k-publish");
    writer.write(&event).await.unwrap();

    let bodies = drain_bodies(&stack.sqs, &queue_url, 1, Duration::from_secs(30)).await;
    assert_eq!(bodies.len(), 1, "expected exactly one delivered message");

    let delivered = decode_event(&bodies[0]);
    assert_eq!(delivered.id(), event.id());
    assert_eq!(delivered.key().as_str(), "k-publish");
    assert_eq!(delivered.topic().as_str(), "thing.happened");
    assert_eq!(delivered.organization().as_str(), "orgsns");
    assert_eq!(delivered.namespace().as_str(), "/x");
}

#[tokio::test]
async fn publish_batch_chunks_across_the_ten_entry_limit() {
    let stack = start_localstack().await;
    let topic_arn = create_topic(&stack.sns, "t-batch").await;
    let queue_url = create_queue(&stack.sqs, "q-batch").await;
    subscribe_queue(&stack.sns, &stack.sqs, &topic_arn, &queue_url, true).await;

    let writer = SnsWriter::new(stack.sns.clone(), &topic_arn);

    let events: Vec<Event> = (0..25)
        .map(|i| make_event("orgsns", &format!("k-batch-{i}")))
        .collect();
    writer.write_all(&events).await.unwrap();

    let bodies = drain_bodies(&stack.sqs, &queue_url, 25, Duration::from_secs(60)).await;
    assert_eq!(bodies.len(), 25, "every batched event should be delivered");

    let delivered: HashSet<String> = bodies
        .iter()
        .map(|b| decode_event(b).key().as_str().to_owned())
        .collect();
    let expected: HashSet<String> = (0..25).map(|i| format!("k-batch-{i}")).collect();
    assert_eq!(
        delivered, expected,
        "no event lost or duplicated at a batch boundary"
    );
}

#[tokio::test]
async fn publish_fans_out_to_every_subscribed_queue() {
    let stack = start_localstack().await;
    let topic_arn = create_topic(&stack.sns, "t-fanout").await;
    let projection_url = create_queue(&stack.sqs, "q-fanout-projection").await;
    let audit_url = create_queue(&stack.sqs, "q-fanout-audit").await;
    subscribe_queue(&stack.sns, &stack.sqs, &topic_arn, &projection_url, true).await;
    subscribe_queue(&stack.sns, &stack.sqs, &topic_arn, &audit_url, true).await;

    let writer = SnsWriter::new(stack.sns.clone(), &topic_arn);
    let event = make_event("orgsns", "k-fanout");
    writer.write(&event).await.unwrap();

    let projection = drain_bodies(&stack.sqs, &projection_url, 1, Duration::from_secs(30)).await;
    let audit = drain_bodies(&stack.sqs, &audit_url, 1, Duration::from_secs(30)).await;

    assert_eq!(projection.len(), 1, "projection queue received the event");
    assert_eq!(audit.len(), 1, "audit queue received the event");
    assert_eq!(decode_event(&projection[0]).id(), event.id());
    assert_eq!(decode_event(&audit[0]).id(), event.id());
}

#[tokio::test]
async fn sns_to_sqs_reader_round_trips_end_to_end() {
    let stack = start_localstack().await;
    let topic_arn = create_topic(&stack.sns, "t-e2e").await;
    let queue_url = create_queue(&stack.sqs, "q-e2e").await;
    subscribe_queue(&stack.sns, &stack.sqs, &topic_arn, &queue_url, true).await;

    let writer = SnsWriter::new(stack.sns.clone(), &topic_arn);
    let event = make_event("orgsns", "k-e2e");
    writer.write(&event).await.unwrap();

    let mut config = SqsReaderConfig::defaults_for(&queue_url);
    config.ack_buffer = AckBufferConfig {
        max_pending: 1,
        flush_interval: Duration::from_millis(50),
    };
    let reader = SqsReader::new(stack.sqs.clone(), config).unwrap();
    let mut stream = reader.read().await.unwrap();

    let message = tokio::time::timeout(Duration::from_secs(30), stream.next())
        .await
        .expect("reader produced a message before timeout")
        .expect("stream not exhausted")
        .expect("message is not an error");
    assert_eq!(message.event().id(), event.id());
    assert_eq!(message.event().key().as_str(), "k-e2e");
    message.ack().await.unwrap();
    drop(stream);

    common::wait_for_message_count(&stack.sqs, &queue_url, 0, Duration::from_secs(20)).await;
}

#[tokio::test]
async fn without_raw_message_delivery_the_body_is_an_undecodable_envelope() {
    let stack = start_localstack().await;
    let topic_arn = create_topic(&stack.sns, "t-envelope").await;
    let queue_url = create_queue(&stack.sqs, "q-envelope").await;
    subscribe_queue(&stack.sns, &stack.sqs, &topic_arn, &queue_url, false).await;

    let writer = SnsWriter::new(stack.sns.clone(), &topic_arn);
    writer
        .write(&make_event("orgsns", "k-envelope"))
        .await
        .unwrap();

    let body = peek_one_body(&stack.sqs, &queue_url, Duration::from_secs(30))
        .await
        .expect("a message was delivered");

    let envelope: serde_json::Value = serde_json::from_str(&body).expect("envelope is JSON");
    assert_eq!(
        envelope["Type"], "Notification",
        "SNS wraps the body when raw delivery is off"
    );

    assert!(
        SerializedEvent::from_json_str(&body).is_err(),
        "the envelope must not decode as a SerializedEvent"
    );

    let inner = envelope["Message"].as_str().expect("Message is a string");
    assert_eq!(decode_event(inner).key().as_str(), "k-envelope");
}

#[tokio::test]
async fn fifo_topic_publish_round_trips_with_group_and_dedup_ids() {
    let stack = start_localstack().await;
    let topic_arn = create_fifo_topic(&stack.sns, "t-fifo.fifo").await;
    let queue_url = create_fifo_queue(&stack.sqs, "q-fifo.fifo").await;
    subscribe_queue(&stack.sns, &stack.sqs, &topic_arn, &queue_url, true).await;

    let writer = SnsWriter::new_with_config(
        stack.sns.clone(),
        &topic_arn,
        SnsWriterConfig {
            fifo: SnsFifoConfig::Fifo,
        },
    );

    let first = make_event("orgsns", "order-1");
    let second = make_event("orgsns", "order-1");
    writer.write(&first).await.unwrap();
    writer.write(&second).await.unwrap();

    let bodies = drain_bodies(&stack.sqs, &queue_url, 2, Duration::from_secs(60)).await;
    assert_eq!(bodies.len(), 2, "both FIFO publishes were delivered");

    let ids: Vec<_> = bodies.iter().map(|b| decode_event(b).id()).collect();
    assert_eq!(
        ids,
        vec![first.id(), second.id()],
        "FIFO preserves publish order within a message group"
    );
}

#[tokio::test]
async fn fifo_batch_publish_round_trips() {
    let stack = start_localstack().await;
    let topic_arn = create_fifo_topic(&stack.sns, "t-fifo-batch.fifo").await;
    let queue_url = create_fifo_queue(&stack.sqs, "q-fifo-batch.fifo").await;
    subscribe_queue(&stack.sns, &stack.sqs, &topic_arn, &queue_url, true).await;

    let writer = SnsWriter::new_with_config(
        stack.sns.clone(),
        &topic_arn,
        SnsWriterConfig {
            fifo: SnsFifoConfig::Fifo,
        },
    );

    let events: Vec<Event> = (0..12)
        .map(|i| make_event("orgsns", &format!("order-{i}")))
        .collect();
    writer.write_all(&events).await.unwrap();

    let bodies = drain_bodies(&stack.sqs, &queue_url, 12, Duration::from_secs(60)).await;
    assert_eq!(bodies.len(), 12, "batch crossed the 10-entry limit intact");
}

#[tokio::test]
async fn oversized_event_is_rejected_before_publishing() {
    let stack = start_localstack().await;
    let topic_arn = create_topic(&stack.sns, "t-oversize").await;
    let queue_url = create_queue(&stack.sqs, "q-oversize").await;
    subscribe_queue(&stack.sns, &stack.sqs, &topic_arn, &queue_url, true).await;

    let writer = SnsWriter::new(stack.sns.clone(), &topic_arn);
    let event = Event::create(
        "orgsns",
        "/x",
        "thing.happened",
        "k-oversize",
        Payload::from_string("x".repeat(300 * 1024)),
    )
    .expect("valid event");

    let err = writer.write(&event).await.unwrap_err();
    assert!(
        matches!(err, Error::InvalidPayload(_)),
        "oversized payloads fail locally, not at the API: {err:?}"
    );

    assert!(
        peek_one_body(&stack.sqs, &queue_url, Duration::from_secs(5))
            .await
            .is_none()
    );
}

#[tokio::test]
async fn write_all_of_nothing_is_a_no_op() {
    let stack = start_localstack().await;
    let topic_arn = create_topic(&stack.sns, "t-empty").await;
    let writer = SnsWriter::new(stack.sns.clone(), &topic_arn);
    writer.write_all(&[]).await.unwrap();
}
