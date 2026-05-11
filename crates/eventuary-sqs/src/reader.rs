use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use aws_sdk_sqs::Client;
use futures::Stream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use eventuary::io::acker::{AckBuffer, Acker, BatchedAcker};
use eventuary::io::{Message, Reader};
use eventuary::{Result, SerializedEvent};

use crate::flusher::SqsFlusher;
use crate::reader_config::SqsReaderConfig;

pub struct SqsReader {
    client: Client,
    config: SqsReaderConfig,
}

impl SqsReader {
    pub fn new(client: Client, config: SqsReaderConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self { client, config })
    }
}

pub struct SqsStream {
    rx: mpsc::Receiver<Result<Message<BatchedAcker<String>>>>,
    cancel: CancellationToken,
    handle: Option<tokio::task::JoinHandle<()>>,
    ack_buffer: Arc<AckBuffer<SqsFlusher>>,
}

impl Drop for SqsStream {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(h) = self.handle.take() {
            h.abort();
        }
        let buf = Arc::clone(&self.ack_buffer);
        tokio::spawn(async move {
            let _ = buf.shutdown().await;
        });
    }
}

impl Stream for SqsStream {
    type Item = Result<Message<BatchedAcker<String>>>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

impl Reader for SqsReader {
    type Acker = BatchedAcker<String>;
    type Stream = SqsStream;

    async fn read(&self) -> Result<Self::Stream> {
        let client = self.client.clone();
        let config = self.config.clone();
        let flusher = SqsFlusher::new(client.clone(), config.queue_url.clone());
        let ack_buffer = AckBuffer::spawn(flusher, config.ack_buffer.clone());
        let tx_ack = ack_buffer.sender();

        let (tx, rx) = mpsc::channel((config.max_messages as usize) * 2);
        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();

        let handle = tokio::spawn(async move {
            loop {
                if cancel_for_task.is_cancelled() {
                    break;
                }
                let resp = client
                    .receive_message()
                    .queue_url(&config.queue_url)
                    .max_number_of_messages(config.max_messages)
                    .wait_time_seconds(config.wait_time.as_secs() as i32)
                    .visibility_timeout(config.visibility_timeout.as_secs() as i32)
                    .send()
                    .await;
                let messages = match resp {
                    Ok(o) => o.messages.unwrap_or_default(),
                    Err(e) => {
                        tracing::warn!("sqs receive error: {e}");
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_secs(1)) => continue,
                            _ = cancel_for_task.cancelled() => return,
                        }
                    }
                };
                for m in messages {
                    let body = match m.body.as_deref() {
                        Some(b) => b,
                        None => continue,
                    };
                    let receipt = match m.receipt_handle.clone() {
                        Some(r) => r,
                        None => continue,
                    };
                    let serialized = match SerializedEvent::from_json_str(body) {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::warn!("malformed sqs body: {e}");
                            continue;
                        }
                    };
                    if serialized.organization != config.organization.as_str() {
                        tracing::warn!(
                            "sqs org mismatch: got `{}` expected `{}`; acking and skipping",
                            serialized.organization,
                            config.organization
                        );
                        let skip_acker = BatchedAcker::new(receipt, tx_ack.clone());
                        let _ = skip_acker.ack().await;
                        continue;
                    }
                    if !config.topics.is_empty()
                        && !config.topics.iter().any(|t| t.as_str() == serialized.topic)
                    {
                        let skip_acker = BatchedAcker::new(receipt, tx_ack.clone());
                        let _ = skip_acker.ack().await;
                        continue;
                    }
                    if let Some(ns) = config.namespace.as_ref()
                        && !ns.is_root()
                    {
                        let event_ns = serialized.namespace.as_str();
                        let prefix = ns.as_str();
                        let matches_ns =
                            event_ns == prefix || event_ns.starts_with(&format!("{prefix}/"));
                        if !matches_ns {
                            let skip_acker = BatchedAcker::new(receipt, tx_ack.clone());
                            let _ = skip_acker.ack().await;
                            continue;
                        }
                    }
                    let event = match serialized.to_event() {
                        Ok(e) => e,
                        Err(e) => {
                            tracing::warn!("sqs to_event failed: {e}");
                            continue;
                        }
                    };
                    let acker = BatchedAcker::new(receipt, tx_ack.clone());
                    if tx.send(Ok(Message::new(event, acker))).await.is_err() {
                        return;
                    }
                }
            }
        });

        Ok(SqsStream {
            rx,
            cancel,
            handle: Some(handle),
            ack_buffer,
        })
    }
}
