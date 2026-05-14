use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use aws_sdk_sqs::Client;
use futures::Stream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use eventuary_core::io::acker::{AckBuffer, Acker, BatchedAcker};
use eventuary_core::io::{Message, NoCursor, Reader};
use eventuary_core::{Result, SerializedEvent, StartFrom, StartableSubscription};

use crate::flusher::SqsFlusher;
use crate::reader_config::SqsReaderConfig;

#[derive(Debug, Clone)]
pub struct SqsSubscription {
    pub queue_url: String,
    pub wait_time: Duration,
    pub visibility_timeout: Duration,
    pub max_messages: i32,
    pub limit: Option<usize>,
}

impl StartableSubscription<NoCursor> for SqsSubscription {
    fn with_start(self, _: StartFrom<NoCursor>) -> Self {
        self
    }
}

pub struct SqsReader {
    client: Client,
    config: SqsReaderConfig,
}

impl SqsReader {
    pub fn new(client: Client, config: SqsReaderConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self { client, config })
    }

    pub fn default_subscription(&self) -> SqsSubscription {
        SqsSubscription {
            queue_url: self.config.queue_url.clone(),
            wait_time: self.config.wait_time,
            visibility_timeout: self.config.visibility_timeout,
            max_messages: self.config.max_messages,
            limit: self.config.limit,
        }
    }

    pub async fn read(&self) -> Result<SqsStream> {
        eventuary_core::io::Reader::read(self, self.default_subscription()).await
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
    type Subscription = SqsSubscription;
    type Acker = BatchedAcker<String>;
    type Cursor = eventuary_core::io::NoCursor;
    type Stream = SqsStream;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let client = self.client.clone();
        let queue_url = subscription.queue_url.clone();
        let max_messages = subscription.max_messages;
        let wait_time = subscription.wait_time;
        let visibility_timeout = subscription.visibility_timeout;
        let limit = subscription.limit;

        let flusher = SqsFlusher::new(client.clone(), queue_url.clone());
        let ack_buffer = AckBuffer::spawn(flusher, self.config.ack_buffer.clone());
        let tx_ack = ack_buffer.sender();

        let (tx, rx) = mpsc::channel((max_messages as usize) * 2);
        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();

        let handle = tokio::spawn(async move {
            let mut delivered = 0usize;
            loop {
                if cancel_for_task.is_cancelled() {
                    break;
                }
                let resp = client
                    .receive_message()
                    .queue_url(&queue_url)
                    .max_number_of_messages(max_messages)
                    .wait_time_seconds(wait_time.as_secs() as i32)
                    .visibility_timeout(visibility_timeout.as_secs() as i32)
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
                    let receipt = match m.receipt_handle.clone() {
                        Some(r) => r,
                        None => continue,
                    };
                    let body = match m.body.as_deref() {
                        Some(b) => b,
                        None => {
                            let _ = BatchedAcker::new(receipt, tx_ack.clone()).ack().await;
                            continue;
                        }
                    };
                    let serialized = match SerializedEvent::from_json_str(body) {
                        Ok(s) => s,
                        Err(_) => {
                            let _ = BatchedAcker::new(receipt, tx_ack.clone()).ack().await;
                            continue;
                        }
                    };
                    let event = match serialized.to_event() {
                        Ok(e) => e,
                        Err(_) => {
                            let _ = BatchedAcker::new(receipt, tx_ack.clone()).ack().await;
                            continue;
                        }
                    };
                    let acker = BatchedAcker::new(receipt, tx_ack.clone());
                    if tx
                        .send(Ok(Message::new(event, acker, eventuary_core::io::NoCursor)))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    delivered += 1;
                    if let Some(l) = limit
                        && delivered >= l
                    {
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
