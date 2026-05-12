use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::{Stream, StreamExt};
use rdkafka::ClientConfig;
use rdkafka::Message as KafkaMessage;
use rdkafka::TopicPartitionList;
use rdkafka::consumer::{Consumer, StreamConsumer};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use eventuary_core::io::acker::{AckBuffer, Acker, BatchedAcker};
use eventuary_core::io::{Message, Reader};
use eventuary_core::{
    Error, Event, EventSubscription, OrganizationId, Result, SerializedEvent, StartFrom,
};

use crate::flusher::{KafkaFlusher, KafkaOffsetToken};
use crate::reader_config::KafkaReaderConfig;

pub struct KafkaReader {
    consumer: Arc<StreamConsumer>,
    config: KafkaReaderConfig,
}

impl KafkaReader {
    pub fn new(config: KafkaReaderConfig) -> Result<Self> {
        config.validate()?;
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", config.brokers.join(","))
            .set("group.id", config.consumer_group_id.as_str())
            .set("enable.auto.commit", "false")
            .set(
                "session.timeout.ms",
                config.session_timeout.as_millis().to_string(),
            )
            .set(
                "auto.offset.reset",
                match config.start_from {
                    StartFrom::Earliest => "earliest",
                    StartFrom::Latest | StartFrom::Timestamp(_) => "latest",
                },
            );
        let consumer: StreamConsumer = cfg.create().map_err(|e| Error::Store(e.to_string()))?;

        let topic_refs: Vec<&str> = config.kafka_topics.iter().map(|t| t.as_str()).collect();
        consumer
            .subscribe(&topic_refs)
            .map_err(|e| Error::Store(e.to_string()))?;

        if let StartFrom::Timestamp(ts) = config.start_from {
            apply_timestamp_seek(&consumer, &config, ts)?;
        }

        Ok(Self {
            consumer: Arc::new(consumer),
            config,
        })
    }

    /// Build an [`EventSubscription`] seeded from this reader's config: the
    /// configured tenant, consumer group, event-topic filter, namespace
    /// prefix, `start_from`, `end_at`, and `limit`. Use it as a starting
    /// point and tweak before passing to [`Reader::read`].
    pub fn default_subscription(&self) -> EventSubscription {
        let mut subscription = match &self.config.organization {
            Some(org) => EventSubscription::for_organization(org.clone()),
            None => EventSubscription::new(),
        };
        subscription.consumer_group_id = Some(self.config.consumer_group_id.clone());
        subscription.topics = self.config.event_topics.clone();
        subscription.namespace_prefix = self.config.namespace.clone();
        subscription.start_from = self.config.start_from;
        subscription.end_at = self.config.end_at;
        subscription.limit = self.config.limit;
        subscription
    }

    /// Convenience entry point that reads with [`default_subscription`].
    /// Equivalent to `Reader::read(self, self.default_subscription())`.
    ///
    /// [`default_subscription`]: KafkaReader::default_subscription
    pub async fn read(&self) -> Result<KafkaStream> {
        eventuary_core::io::Reader::read(self, self.default_subscription()).await
    }
}

fn apply_timestamp_seek(
    consumer: &StreamConsumer,
    config: &KafkaReaderConfig,
    ts: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    let mut tpl = TopicPartitionList::new();
    for topic in &config.kafka_topics {
        let metadata = consumer
            .fetch_metadata(Some(topic), Duration::from_secs(10))
            .map_err(|e| Error::Store(e.to_string()))?;
        let topic_meta = metadata
            .topics()
            .iter()
            .find(|t| t.name() == topic)
            .ok_or_else(|| Error::Store(format!("topic {topic} not found")))?;
        for p in topic_meta.partitions() {
            tpl.add_partition_offset(
                topic,
                p.id(),
                rdkafka::Offset::Offset(ts.timestamp_millis()),
            )
            .map_err(|e| Error::Store(e.to_string()))?;
        }
    }
    let resolved = consumer
        .offsets_for_times(tpl, Duration::from_secs(10))
        .map_err(|e| Error::Store(e.to_string()))?;
    consumer
        .assign(&resolved)
        .map_err(|e| Error::Store(e.to_string()))?;
    Ok(())
}

pub struct KafkaStream {
    rx: mpsc::Receiver<Result<Message<BatchedAcker<KafkaOffsetToken>>>>,
    cancel: CancellationToken,
    handle: Option<tokio::task::JoinHandle<()>>,
    ack_buffer: Arc<AckBuffer<KafkaFlusher>>,
}

impl Drop for KafkaStream {
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

impl Stream for KafkaStream {
    type Item = Result<Message<BatchedAcker<KafkaOffsetToken>>>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
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

impl Reader for KafkaReader {
    type Subscription = EventSubscription;
    type Acker = BatchedAcker<KafkaOffsetToken>;
    type Stream = KafkaStream;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        validate_organization_filter(&subscription, &self.config.organization)?;
        if let Some(group) = subscription.consumer_group_id.as_ref()
            && group != &self.config.consumer_group_id
        {
            return Err(Error::Config(format!(
                "subscription consumer_group_id `{}` does not match reader group `{}` (Kafka group is bound at construction)",
                group.as_str(),
                self.config.consumer_group_id.as_str()
            )));
        }
        if subscription.start_from != self.config.start_from {
            return Err(Error::Config(
                "subscription start_from cannot override reader; Kafka group offset is bound at construction".to_owned(),
            ));
        }

        let consumer = Arc::clone(&self.consumer);
        let flusher = KafkaFlusher::new(Arc::clone(&consumer));
        let ack_buffer = AckBuffer::spawn(flusher, self.config.ack_buffer.clone());
        let tx_ack = ack_buffer.sender();

        let (tx, rx) = mpsc::channel(self.config.max_poll_records);
        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();

        let handle = tokio::spawn(async move {
            let mut delivered = 0usize;
            let mut stream = consumer.stream();

            loop {
                if cancel_for_task.is_cancelled() {
                    break;
                }
                let next = tokio::select! {
                    n = stream.next() => n,
                    _ = cancel_for_task.cancelled() => return,
                };
                let msg = match next {
                    None => return,
                    Some(Err(e)) => {
                        tracing::warn!("kafka stream error: {e}");
                        continue;
                    }
                    Some(Ok(m)) => m,
                };
                let token = KafkaOffsetToken {
                    topic: msg.topic().to_owned(),
                    partition: msg.partition(),
                    offset: msg.offset(),
                };
                let body = match msg.payload() {
                    Some(p) => p,
                    None => {
                        tracing::warn!(
                            topic = %token.topic,
                            partition = token.partition,
                            offset = token.offset,
                            "kafka record missing payload; skip-acking"
                        );
                        let _ = BatchedAcker::new(token, tx_ack.clone()).ack().await;
                        continue;
                    }
                };
                let serialized: SerializedEvent = match serde_json::from_slice(body) {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(
                            topic = %token.topic,
                            partition = token.partition,
                            offset = token.offset,
                            error = %e,
                            "kafka body parse error; skip-acking poison record"
                        );
                        let _ = BatchedAcker::new(token, tx_ack.clone()).ack().await;
                        continue;
                    }
                };
                let event: Event = match serialized.to_event() {
                    Ok(e) => e,
                    Err(e) => {
                        tracing::warn!(
                            topic = %token.topic,
                            partition = token.partition,
                            offset = token.offset,
                            error = %e,
                            "kafka to_event error; skip-acking poison record"
                        );
                        let _ = BatchedAcker::new(token, tx_ack.clone()).ack().await;
                        continue;
                    }
                };
                if !subscription.matches(&event) {
                    let _ = BatchedAcker::new(token, tx_ack.clone()).ack().await;
                    continue;
                }
                let acker = BatchedAcker::new(token, tx_ack.clone());
                if tx.send(Ok(Message::new(event, acker))).await.is_err() {
                    return;
                }
                delivered += 1;
                if let Some(l) = subscription.limit
                    && delivered >= l
                {
                    return;
                }
            }
        });

        Ok(KafkaStream {
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
}
