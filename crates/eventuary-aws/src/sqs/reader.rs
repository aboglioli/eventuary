pub use crate::sqs::reader_config::SqsReaderConfig;
pub use crate::sqs::subscription::SqsSubscription;

use std::time::Duration;

use aws_sdk_sqs::Client;
use uuid::Uuid;

use eventuary_core::io::acker::{Acker, BatchedAcker};
use eventuary_core::io::stream::BatchedStream;
use eventuary_core::io::{Message, NoCursor, Reader};
use eventuary_core::{Error, Event, Result, SerializedEvent};

use crate::sqs::flusher::SqsFlusher;

fn decode_event(body: Option<&str>) -> Result<Event> {
    let body = body.ok_or_else(|| Error::Serialization("message has no body".to_owned()))?;
    SerializedEvent::from_json_str(body)?.to_event()
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
        self.config.subscription()
    }

    pub async fn read(&self) -> Result<BatchedStream<String, SqsFlusher>> {
        Reader::read(self, self.default_subscription()).await
    }
}

impl Reader for SqsReader {
    type Subscription = SqsSubscription;
    type Acker = BatchedAcker<String>;
    type Cursor = NoCursor;
    type Stream = BatchedStream<String, SqsFlusher>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        subscription.validate()?;
        let client = self.client.clone();
        let queue_url = subscription.queue_url.clone();
        let max_messages = subscription.max_messages;
        let wait_time = subscription.wait_time;
        let visibility_timeout = subscription.visibility_timeout;
        let limit = subscription.limit;
        let queue_type = subscription.queue_type;

        Ok(BatchedStream::spawn(
            SqsFlusher::new(client.clone(), queue_url.clone()),
            self.config.ack_buffer.clone(),
            (max_messages as usize) * 2,
            move |tx, tx_ack, cancel| {
                Box::pin(async move {
                    let mut delivered = 0usize;
                    let mut receive_attempt_id: Option<String> = None;
                    loop {
                        let mut request = client
                            .receive_message()
                            .queue_url(&queue_url)
                            .max_number_of_messages(max_messages)
                            .wait_time_seconds(wait_time.as_secs() as i32)
                            .visibility_timeout(visibility_timeout.as_secs() as i32);
                        if queue_type.is_fifo() {
                            let attempt_id = receive_attempt_id
                                .get_or_insert_with(|| Uuid::now_v7().to_string())
                                .clone();
                            request = request.receive_request_attempt_id(attempt_id);
                        }
                        let resp = request.send().await;
                        let messages = match resp {
                            Ok(o) => {
                                receive_attempt_id = None;
                                o.messages.unwrap_or_default()
                            }
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
                            let event = match decode_event(m.body.as_deref()) {
                                Ok(event) => event,
                                Err(error) => {
                                    tracing::warn!(
                                        queue_url = %queue_url,
                                        message_id = m.message_id.as_deref().unwrap_or("<none>"),
                                        %error,
                                        "deleting undecodable SQS message"
                                    );
                                    let _ = BatchedAcker::new(receipt, tx_ack.clone()).ack().await;
                                    continue;
                                }
                            };
                            let acker = BatchedAcker::new(receipt, tx_ack.clone());
                            if tx
                                .send(Ok(Message::new(event, acker, NoCursor)))
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
