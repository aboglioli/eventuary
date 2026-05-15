use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use rdkafka::ClientConfig;
use rdkafka::Message as KafkaMessage;
use rdkafka::TopicPartitionList;
use rdkafka::consumer::{Consumer, StreamConsumer};

use eventuary_core::io::acker::{Acker, BatchedAcker};
use eventuary_core::io::stream::BatchedStream;
use eventuary_core::io::{Cursor, Message, Reader};

use crate::flusher::KafkaFlusher;
use eventuary_core::{
    ConsumerGroupId, Error, Event, Result, SerializedEvent, StartFrom, StartableSubscription,
};

use crate::flusher::KafkaOffsetToken;
use crate::reader_config::KafkaReaderConfig;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct KafkaCursor {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

impl PartialOrd for KafkaCursor {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KafkaCursor {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.topic
            .cmp(&other.topic)
            .then(self.partition.cmp(&other.partition))
            .then(self.offset.cmp(&other.offset))
    }
}

impl Cursor for KafkaCursor {}

#[derive(Debug, Clone)]
pub struct KafkaSubscription {
    pub topics: Vec<String>,
    pub consumer_group_id: ConsumerGroupId,
    pub start_from: StartFrom<KafkaCursor>,
    pub limit: Option<usize>,
}

impl StartableSubscription<KafkaCursor> for KafkaSubscription {
    fn with_start(mut self, start: StartFrom<KafkaCursor>) -> Self {
        self.start_from = start;
        self
    }
}

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
                match &config.start_from {
                    StartFrom::Earliest => "earliest",
                    StartFrom::Latest | StartFrom::Timestamp(_) | StartFrom::After(_) => "latest",
                },
            );
        let consumer: StreamConsumer = cfg.create().map_err(|e| Error::Store(e.to_string()))?;

        let topic_refs: Vec<&str> = config.kafka_topics.iter().map(|t| t.as_str()).collect();
        consumer
            .subscribe(&topic_refs)
            .map_err(|e| Error::Store(e.to_string()))?;

        if let StartFrom::Timestamp(ts) = &config.start_from {
            apply_timestamp_seek(&consumer, &config, *ts)?;
        }

        Ok(Self {
            consumer: Arc::new(consumer),
            config,
        })
    }

    pub fn default_subscription(&self) -> KafkaSubscription {
        KafkaSubscription {
            topics: self.config.kafka_topics.clone(),
            consumer_group_id: self.config.consumer_group_id.clone(),
            start_from: StartFrom::Latest,
            limit: self.config.limit,
        }
    }

    pub async fn read(&self) -> Result<BatchedStream<KafkaOffsetToken, KafkaFlusher, KafkaCursor>> {
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

impl Reader for KafkaReader {
    type Subscription = KafkaSubscription;
    type Acker = BatchedAcker<KafkaOffsetToken>;
    type Cursor = KafkaCursor;
    type Stream = BatchedStream<KafkaOffsetToken, KafkaFlusher, KafkaCursor>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        if subscription.consumer_group_id != self.config.consumer_group_id {
            return Err(Error::Config(format!(
                "subscription consumer_group_id `{}` does not match reader group `{}` (Kafka group is bound at construction)",
                subscription.consumer_group_id.as_str(),
                self.config.consumer_group_id.as_str()
            )));
        }
        if !matches!(subscription.start_from, StartFrom::Latest)
            && (matches!(&self.config.start_from, StartFrom::Latest)
                != matches!(subscription.start_from, StartFrom::Latest))
        {
            return Err(Error::Config(
                "subscription start_from cannot override reader; Kafka group offset is bound at construction".to_owned(),
            ));
        }

        let consumer = Arc::clone(&self.consumer);
        let max_poll_records = self.config.max_poll_records;
        let limit = subscription.limit;

        Ok(BatchedStream::spawn(
            KafkaFlusher::new(Arc::clone(&consumer)),
            self.config.ack_buffer.clone(),
            max_poll_records,
            move |tx, tx_ack, cancel| {
                Box::pin(async move {
                    let mut delivered = 0usize;
                    let mut stream = consumer.stream();
                    loop {
                        let next = tokio::select! {
                            n = stream.next() => n,
                            _ = cancel.cancelled() => return,
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
                                let _ = BatchedAcker::new(token, tx_ack.clone()).ack().await;
                                continue;
                            }
                        };
                        let serialized: SerializedEvent = match serde_json::from_slice(body) {
                            Ok(s) => s,
                            Err(_) => {
                                let _ = BatchedAcker::new(token, tx_ack.clone()).ack().await;
                                continue;
                            }
                        };
                        let event: Event = match serialized.to_event() {
                            Ok(e) => e,
                            Err(_) => {
                                let _ = BatchedAcker::new(token, tx_ack.clone()).ack().await;
                                continue;
                            }
                        };
                        let cursor = KafkaCursor {
                            topic: token.topic.clone(),
                            partition: token.partition,
                            offset: token.offset,
                        };
                        let acker = BatchedAcker::new(token, tx_ack.clone());
                        if tx
                            .send(Ok(Message::new(event, acker, cursor)))
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
                })
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventuary_core::io::{Cursor, CursorId};

    #[test]
    fn kafka_cursor_id_is_global() {
        let cursor = KafkaCursor {
            topic: "events".to_owned(),
            partition: 0,
            offset: 42,
        };
        assert_eq!(cursor.id(), CursorId::Global);
    }
}
