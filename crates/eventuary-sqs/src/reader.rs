use std::sync::Arc;
use std::time::Duration;

use aws_sdk_sqs::Client;

use eventuary_core::io::acker::{Acker, BatchedAcker};
use eventuary_core::io::{BatchedStream, Message, Reader, batched_source};
use eventuary_core::{Result, SerializedEvent};

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

    pub async fn read(&self) -> Result<BatchedStream<String>> {
        eventuary_core::io::Reader::read(self, self.default_subscription()).await
    }
}

impl Reader for SqsReader {
    type Subscription = SqsSubscription;
    type Acker = BatchedAcker<String>;
    type Cursor = eventuary_core::io::NoCursor;
    type Stream = BatchedStream<String>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let client = self.client.clone();
        let queue_url = subscription.queue_url.clone();
        let max_messages = subscription.max_messages;
        let wait_time = subscription.wait_time;
        let visibility_timeout = subscription.visibility_timeout;
        let limit = subscription.limit;

        Ok(batched_source(
            SqsFlusher::new(client.clone(), queue_url.clone()),
            self.config.ack_buffer.clone(),
            (max_messages as usize) * 2,
            move |tx, tx_ack, cancel| {
                Box::pin(async move {
                    let mut delivered = 0usize;
                    loop {
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
                                    _ = cancel.cancelled() => return,
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
                                    let _ =
                                        BatchedAcker::new(receipt, tx_ack.clone()).ack().await;
                                    continue;
                                }
                            };
                            let serialized = match SerializedEvent::from_json_str(body) {
                                Ok(s) => s,
                                Err(_) => {
                                    let _ =
                                        BatchedAcker::new(receipt, tx_ack.clone()).ack().await;
                                    continue;
                                }
                            };
                            let event = match serialized.to_event() {
                                Ok(e) => e,
                                Err(_) => {
                                    let _ =
                                        BatchedAcker::new(receipt, tx_ack.clone()).ack().await;
                                    continue;
                                }
                            };
                            let acker = BatchedAcker::new(receipt, tx_ack.clone());
                            if tx
                                .send(Ok(Message::new(
                                    event,
                                    acker,
                                    eventuary_core::io::NoCursor,
                                )))
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
                })
            },
        ))
    }
}
