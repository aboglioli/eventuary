mod common;

use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::time::Duration;

use futures::StreamExt;

use eventuary_core::io::Writer;
use eventuary_core::io::acker::{AckBufferConfig, BatchFlusher};
use eventuary_core::{Error, Event};

use eventuary_aws::sqs::flusher::SqsFlusher;
use eventuary_aws::sqs::queue::SqsQueueType;
use eventuary_aws::sqs::reader::{SqsReader, SqsReaderConfig};
use eventuary_aws::sqs::writer::{SqsWriter, SqsWriterConfig};

use common::{
    approximate_messages, create_fifo_queue, create_queue, decode_event, drain_bodies, make_event,
    send_raw, start_aws_emulator, wait_for_message_count,
};

async fn receive_receipts(
    client: &aws_sdk_sqs::Client,
    queue_url: &str,
    max: i32,
    visibility_timeout: i32,
) -> Vec<String> {
    let resp = client
        .receive_message()
        .queue_url(queue_url)
        .max_number_of_messages(max)
        .wait_time_seconds(10)
        .visibility_timeout(visibility_timeout)
        .send()
        .await
        .expect("receive message");
    resp.messages
        .unwrap_or_default()
        .into_iter()
        .filter_map(|m| m.receipt_handle)
        .collect()
}

#[tokio::test]
async fn writer_sends_serialized_event() {
    let aws = start_aws_emulator().await;
    let queue_url = create_queue(&aws.sqs, "q-writer").await;
    let writer = SqsWriter::new(aws.sqs.clone(), &queue_url);
    let event = make_event("orgsqs", "k1");
    writer.write(&event).await.unwrap();

    let bodies = drain_bodies(&aws.sqs, &queue_url, 1, Duration::from_secs(30)).await;
    assert_eq!(bodies.len(), 1);

    let value: serde_json::Value = serde_json::from_str(&bodies[0]).unwrap();
    assert_eq!(value["organization"], "orgsqs");
    assert_eq!(value["topic"], "thing.happened");
    assert_eq!(value["key"], "k1");
    assert_eq!(value["namespace"], "/x");
}

#[tokio::test]
async fn writer_batches_across_the_ten_entry_limit() {
    let aws = start_aws_emulator().await;
    let queue_url = create_queue(&aws.sqs, "q-writer-batch").await;
    let writer = SqsWriter::new(aws.sqs.clone(), &queue_url);

    let events: Vec<Event> = (0..25)
        .map(|i| make_event("orgsqs", &format!("k-batch-{i}")))
        .collect();
    writer.write_all(&events).await.unwrap();

    let bodies = drain_bodies(&aws.sqs, &queue_url, 25, Duration::from_secs(60)).await;
    assert_eq!(bodies.len(), 25);

    let delivered: HashSet<String> = bodies
        .iter()
        .map(|b| decode_event(b).key().as_str().to_owned())
        .collect();
    let expected: HashSet<String> = (0..25).map(|i| format!("k-batch-{i}")).collect();
    assert_eq!(delivered, expected, "no event lost at a batch boundary");
}

#[tokio::test]
async fn writer_write_all_of_nothing_is_a_no_op() {
    let aws = start_aws_emulator().await;
    let queue_url = create_queue(&aws.sqs, "q-writer-empty").await;
    let writer = SqsWriter::new(aws.sqs.clone(), &queue_url);
    writer.write_all(&[]).await.unwrap();
    assert_eq!(approximate_messages(&aws.sqs, &queue_url).await, 0);
}

#[tokio::test]
async fn reader_receives_event() {
    let aws = start_aws_emulator().await;
    let queue_url = create_queue(&aws.sqs, "q-reader").await;
    let writer = SqsWriter::new(aws.sqs.clone(), &queue_url);
    let event = make_event("orgsqs", "k-recv");
    writer.write(&event).await.unwrap();

    let config = SqsReaderConfig::defaults_for(&queue_url);
    let reader = SqsReader::new(aws.sqs.clone(), config).unwrap();
    let mut stream = reader.read().await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(30), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(msg.event().id(), event.id());
    assert_eq!(msg.event().key().as_str(), "k-recv");
    assert_eq!(msg.event().topic().as_str(), "thing.happened");
}

#[tokio::test]
async fn ack_deletes_message() {
    let aws = start_aws_emulator().await;
    let queue_url = create_queue(&aws.sqs, "q-ack").await;
    let writer = SqsWriter::new(aws.sqs.clone(), &queue_url);
    writer.write(&make_event("orgsqs", "k-ack")).await.unwrap();

    let mut config = SqsReaderConfig::defaults_for(&queue_url);
    config.ack_buffer =
        AckBufferConfig::new(NonZeroUsize::new(1).unwrap(), Duration::from_millis(50));
    let reader = SqsReader::new(aws.sqs.clone(), config).unwrap();
    let mut stream = reader.read().await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(30), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(msg.event().key().as_str(), "k-ack");
    msg.ack().await.unwrap();
    drop(stream);

    wait_for_message_count(&aws.sqs, &queue_url, 0, Duration::from_secs(20)).await;
}

#[tokio::test]
async fn reader_acks_and_skips_poison_records() {
    let aws = start_aws_emulator().await;
    let queue_url = create_queue(&aws.sqs, "q-poison").await;

    send_raw(&aws.sqs, &queue_url, "not json at all").await;
    send_raw(
        &aws.sqs,
        &queue_url,
        r#"{"valid":"json","but":"not an event"}"#,
    )
    .await;
    send_raw(&aws.sqs, &queue_url, "{").await;

    let writer = SqsWriter::new(aws.sqs.clone(), &queue_url);
    let good = make_event("orgsqs", "k-good");
    writer.write(&good).await.unwrap();

    let mut config = SqsReaderConfig::defaults_for(&queue_url);
    config.ack_buffer =
        AckBufferConfig::new(NonZeroUsize::new(1).unwrap(), Duration::from_millis(50));
    let reader = SqsReader::new(aws.sqs.clone(), config).unwrap();
    let mut stream = reader.read().await.unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(30), stream.next())
        .await
        .expect("reader delivered the good event before timeout")
        .unwrap()
        .unwrap();
    assert_eq!(msg.event().id(), good.id());
    msg.ack().await.unwrap();

    wait_for_message_count(&aws.sqs, &queue_url, 0, Duration::from_secs(60)).await;
    drop(stream);
}

#[tokio::test]
async fn flusher_ack_deletes_messages_in_a_batch() {
    let aws = start_aws_emulator().await;
    let queue_url = create_queue(&aws.sqs, "q-flush-ack").await;
    let writer = SqsWriter::new(aws.sqs.clone(), &queue_url);
    let events: Vec<Event> = (0..3)
        .map(|i| make_event("orgsqs", &format!("k-flush-{i}")))
        .collect();
    writer.write_all(&events).await.unwrap();

    let receipts = receive_receipts(&aws.sqs, &queue_url, 10, 30).await;
    assert_eq!(receipts.len(), 3, "all three messages received");

    let flusher = SqsFlusher::new(aws.sqs.clone(), &queue_url);
    flusher.flush(receipts).await.unwrap();

    wait_for_message_count(&aws.sqs, &queue_url, 0, Duration::from_secs(20)).await;
}

#[tokio::test]
async fn flusher_nack_restores_visibility_immediately() {
    let aws = start_aws_emulator().await;
    let queue_url = create_queue(&aws.sqs, "q-flush-nack").await;
    let writer = SqsWriter::new(aws.sqs.clone(), &queue_url);
    writer.write(&make_event("orgsqs", "k-nack")).await.unwrap();

    let receipts = receive_receipts(&aws.sqs, &queue_url, 1, 300).await;
    assert_eq!(receipts.len(), 1);

    let flusher = SqsFlusher::new(aws.sqs.clone(), &queue_url);
    flusher.flush_nack(receipts).await.unwrap();

    let bodies = drain_bodies(&aws.sqs, &queue_url, 1, Duration::from_secs(30)).await;
    assert_eq!(bodies.len(), 1, "nack returned the message to the queue");
    assert_eq!(decode_event(&bodies[0]).key().as_str(), "k-nack");
}

#[tokio::test]
async fn flusher_handles_empty_batches() {
    let aws = start_aws_emulator().await;
    let queue_url = create_queue(&aws.sqs, "q-flush-empty").await;
    let flusher = SqsFlusher::new(aws.sqs.clone(), &queue_url);
    flusher.flush(Vec::new()).await.unwrap();
    flusher.flush_nack(Vec::new()).await.unwrap();
}

#[tokio::test]
async fn invalid_wait_time_is_rejected() {
    let mut config = SqsReaderConfig::defaults_for("https://q");
    config.wait_time = Duration::from_secs(21);
    let err = config.validate().unwrap_err();
    assert!(matches!(err, Error::Config(_)));
}

#[tokio::test]
async fn invalid_visibility_timeout_is_rejected() {
    let mut config = SqsReaderConfig::defaults_for("https://q");
    config.visibility_timeout = Duration::from_secs(43_201);
    let err = config.validate().unwrap_err();
    assert!(matches!(err, Error::Config(_)));
}

fn fifo_writer(client: aws_sdk_sqs::Client, queue_url: &str) -> SqsWriter {
    SqsWriter::new_with_config(
        client,
        queue_url,
        SqsWriterConfig {
            queue_type: SqsQueueType::Fifo,
        },
    )
}

#[tokio::test]
async fn writer_sends_to_a_fifo_queue() {
    let aws = start_aws_emulator().await;
    let queue_url = create_fifo_queue(&aws.sqs, "q-fifo-write.fifo").await;
    let writer = fifo_writer(aws.sqs.clone(), &queue_url);

    let event = make_event("orgsqs", "order-1");
    writer.write(&event).await.unwrap();

    let bodies = drain_bodies(&aws.sqs, &queue_url, 1, Duration::from_secs(30)).await;
    assert_eq!(bodies.len(), 1);
    assert_eq!(decode_event(&bodies[0]).id(), event.id());
}

#[tokio::test]
async fn standard_writer_cannot_send_to_a_fifo_queue() {
    let aws = start_aws_emulator().await;
    let queue_url = create_fifo_queue(&aws.sqs, "q-fifo-reject.fifo").await;
    let writer = SqsWriter::new(aws.sqs.clone(), &queue_url);

    let err = writer
        .write(&make_event("orgsqs", "order-1"))
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::Store(_)),
        "FIFO queues reject sends without a MessageGroupId: {err:?}"
    );
}

#[tokio::test]
async fn fifo_writer_batches_and_preserves_group_order() {
    let aws = start_aws_emulator().await;
    let queue_url = create_fifo_queue(&aws.sqs, "q-fifo-batch.fifo").await;
    let writer = fifo_writer(aws.sqs.clone(), &queue_url);

    let events: Vec<Event> = (0..25).map(|_| make_event("orgsqs", "order-1")).collect();
    writer.write_all(&events).await.unwrap();

    let bodies = drain_bodies(&aws.sqs, &queue_url, 25, Duration::from_secs(60)).await;
    assert_eq!(bodies.len(), 25, "batch crossed the 10-entry limit intact");

    let ids: Vec<_> = bodies.iter().map(|b| decode_event(b).id()).collect();
    let expected: Vec<_> = events.iter().map(|e| e.id()).collect();
    assert_eq!(ids, expected, "one message group stays in publish order");
}

#[tokio::test]
async fn reader_consumes_a_fifo_queue() {
    let aws = start_aws_emulator().await;
    let queue_url = create_fifo_queue(&aws.sqs, "q-fifo-read.fifo").await;
    let writer = fifo_writer(aws.sqs.clone(), &queue_url);
    let event = make_event("orgsqs", "order-1");
    writer.write(&event).await.unwrap();

    let mut config = SqsReaderConfig::defaults_for(&queue_url);
    config.queue_type = SqsQueueType::Fifo;
    config.ack_buffer =
        AckBufferConfig::new(NonZeroUsize::new(1).unwrap(), Duration::from_millis(50));
    let reader = SqsReader::new(aws.sqs.clone(), config).unwrap();
    let mut stream = reader.read().await.unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(30), stream.next())
        .await
        .expect("reader produced a message before timeout")
        .unwrap()
        .unwrap();
    assert_eq!(msg.event().id(), event.id());
    msg.ack().await.unwrap();
    drop(stream);

    wait_for_message_count(&aws.sqs, &queue_url, 0, Duration::from_secs(20)).await;
}

#[tokio::test]
async fn fifo_reader_delivers_a_message_group_in_order() {
    let aws = start_aws_emulator().await;
    let queue_url = create_fifo_queue(&aws.sqs, "q-fifo-order.fifo").await;
    let writer = fifo_writer(aws.sqs.clone(), &queue_url);

    let events: Vec<Event> = (0..5).map(|_| make_event("orgsqs", "order-1")).collect();
    writer.write_all(&events).await.unwrap();

    let mut config = SqsReaderConfig::defaults_for(&queue_url);
    config.queue_type = SqsQueueType::Fifo;
    config.ack_buffer =
        AckBufferConfig::new(NonZeroUsize::new(1).unwrap(), Duration::from_millis(50));
    let reader = SqsReader::new(aws.sqs.clone(), config).unwrap();
    let mut stream = reader.read().await.unwrap();

    let mut received = Vec::new();
    for _ in 0..events.len() {
        let msg = tokio::time::timeout(Duration::from_secs(30), stream.next())
            .await
            .expect("reader produced a message before timeout")
            .unwrap()
            .unwrap();
        received.push(msg.event().id());
        msg.ack().await.unwrap();
    }
    drop(stream);

    let expected: Vec<_> = events.iter().map(|e| e.id()).collect();
    assert_eq!(received, expected);
}
