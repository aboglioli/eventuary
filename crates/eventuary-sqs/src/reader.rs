use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use aws_sdk_sqs::Client;
use futures::Stream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use eventuary_core::io::acker::{AckBuffer, Acker, BatchedAcker};
use eventuary_core::io::{Message, Reader};
use eventuary_core::{
    Error, EventSubscription, OrganizationId, Result, SerializedEvent, StartFrom,
};

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

    /// Build an [`EventSubscription`] seeded from this reader's config: the
    /// configured tenant, optional consumer group, topic filter, namespace
    /// prefix, and SQS-compatible `start_from` (always `Latest`). Use it as
    /// a starting point and tweak before passing to [`Reader::read`].
    pub fn default_subscription(&self) -> EventSubscription {
        let mut subscription = match &self.config.organization {
            Some(org) => EventSubscription::for_organization(org.clone()),
            None => EventSubscription::new(),
        };
        subscription.consumer_group_id = self.config.consumer_group_id.clone();
        if !self.config.topics.is_empty() {
            subscription.topics = Some(self.config.topics.clone());
        }
        subscription.namespace_prefix = self.config.namespace.clone();
        subscription.start_from = self.config.start_from;
        subscription.end_at = self.config.end_at;
        subscription.limit = self.config.limit;
        subscription
    }

    /// Convenience entry point that reads with [`default_subscription`].
    /// Equivalent to `Reader::read(self, self.default_subscription())`.
    ///
    /// [`default_subscription`]: SqsReader::default_subscription
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

fn reject_runtime_partition(subscription: &EventSubscription) -> Result<()> {
    if subscription.partition.is_some() {
        return Err(Error::Config(
            "SQS backend does not support runtime partition filter; workers compete on receive and the filter would force each worker to discard most messages".to_owned(),
        ));
    }
    Ok(())
}

fn validate_organization_filter(
    subscription: &EventSubscription,
    configured: &Option<OrganizationId>,
) -> Result<()> {
    if let (Some(sub_org), Some(cfg_org)) = (&subscription.organization, configured)
        && sub_org != cfg_org
    {
        return Err(Error::Config(format!(
            "subscription organization `{:?}` does not match reader tenant `{:?}`",
            subscription.organization, configured
        )));
    }
    Ok(())
}

impl Reader for SqsReader {
    type Subscription = EventSubscription;
    type Acker = BatchedAcker<String>;
    type Stream = SqsStream;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        validate_organization_filter(&subscription, &self.config.organization)?;
        reject_runtime_partition(&subscription)?;
        if !matches!(subscription.start_from, StartFrom::Latest) {
            return Err(Error::Config(
                "SQS reader only supports StartFrom::Latest; queue semantics deliver pending messages and cannot seek".to_owned(),
            ));
        }
        if let Some(group) = subscription.consumer_group_id.as_ref()
            && self.config.consumer_group_id.as_ref() != Some(group)
        {
            return Err(Error::Config(
                "subscription consumer_group_id does not match reader; SQS uses the queue URL as consumer identity".to_owned(),
            ));
        }

        let client = self.client.clone();
        let queue_url = self.config.queue_url.clone();
        let max_messages = self.config.max_messages;
        let wait_time = self.config.wait_time;
        let visibility_timeout = self.config.visibility_timeout;

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
                        None => {
                            tracing::warn!(
                                "sqs message missing receipt handle; cannot ack or delete, dropping"
                            );
                            continue;
                        }
                    };
                    let body = match m.body.as_deref() {
                        Some(b) => b,
                        None => {
                            tracing::warn!("sqs message missing body; skip-acking");
                            let _ = BatchedAcker::new(receipt, tx_ack.clone()).ack().await;
                            continue;
                        }
                    };
                    let serialized = match SerializedEvent::from_json_str(body) {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::warn!(error = %e, "malformed sqs body; skip-acking poison message");
                            let _ = BatchedAcker::new(receipt, tx_ack.clone()).ack().await;
                            continue;
                        }
                    };
                    let event = match serialized.to_event() {
                        Ok(e) => e,
                        Err(e) => {
                            tracing::warn!(error = %e, "sqs to_event failed; skip-acking poison message");
                            let _ = BatchedAcker::new(receipt, tx_ack.clone()).ack().await;
                            continue;
                        }
                    };
                    if !subscription.matches(&event) {
                        let _ = BatchedAcker::new(receipt, tx_ack.clone()).ack().await;
                        continue;
                    }
                    let acker = BatchedAcker::new(receipt, tx_ack.clone());
                    if tx
                        .send(Ok(Message::new(event, acker, eventuary_core::io::NoCursor)))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    delivered += 1;
                    if let Some(limit) = subscription.limit
                        && delivered >= limit
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

#[cfg(test)]
mod tests {
    use super::*;
    use eventuary_core::OrganizationId;

    fn org(value: &str) -> OrganizationId {
        OrganizationId::new(value).unwrap()
    }

    #[test]
    fn organization_filter_allows_all_subscription_with_configured_organization() {
        let subscription = EventSubscription::new();
        let configured = Some(org("acme"));

        validate_organization_filter(&subscription, &configured).unwrap();
    }

    #[test]
    fn organization_filter_allows_scoped_subscription_with_all_organizations_config() {
        let subscription = EventSubscription::for_organization(org("acme"));

        validate_organization_filter(&subscription, &None).unwrap();
    }

    #[test]
    fn organization_filter_rejects_different_scoped_organizations() {
        let subscription = EventSubscription::for_organization(org("acme"));
        let configured = Some(org("other"));

        let err = validate_organization_filter(&subscription, &configured).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn reject_runtime_partition_accepts_unpartitioned() {
        let subscription = EventSubscription::for_organization(org("acme"));
        reject_runtime_partition(&subscription).unwrap();
    }

    #[test]
    fn reject_runtime_partition_rejects_partitioned() {
        use eventuary_core::PartitionAssignment;

        let mut subscription = EventSubscription::for_organization(org("acme"));
        subscription.partition = Some(PartitionAssignment::new(4, 0).unwrap());

        let err = reject_runtime_partition(&subscription).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }
}
