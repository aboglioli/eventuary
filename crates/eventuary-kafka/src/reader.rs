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
    CommitCursor, ConsumerGroupId, CursorPartition, Error, Event, LogicalPartition, Result,
    SerializedEvent, StartFrom, StartableSubscription,
};

use crate::flusher::{KafkaFlusher, KafkaOffsetToken};
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

impl CursorPartition for KafkaCursor {
    fn partition(&self) -> Option<LogicalPartition> {
        None
    }
}

impl CommitCursor for KafkaCursor {
    type Commit = KafkaCursor;
    fn commit_cursor(&self) -> Self::Commit {
        self.clone()
    }
}

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
    rx: mpsc::Receiver<Result<Message<BatchedAcker<KafkaOffsetToken>, KafkaCursor>>>,
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
    type Item = Result<Message<BatchedAcker<KafkaOffsetToken>, KafkaCursor>>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

impl Reader for KafkaReader {
    type Subscription = KafkaSubscription;
    type Acker = BatchedAcker<KafkaOffsetToken>;
    type Cursor = KafkaCursor;
    type Stream = KafkaStream;

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
        let flusher = KafkaFlusher::new(Arc::clone(&consumer));
        let ack_buffer = AckBuffer::spawn(flusher, self.config.ack_buffer.clone());
        let tx_ack = ack_buffer.sender();

        let (tx, rx) = mpsc::channel(self.config.max_poll_records);
        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();
        let limit = subscription.limit;

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
        });

        Ok(KafkaStream {
            rx,
            cancel,
            handle: Some(handle),
            ack_buffer,
        })
    }
}
