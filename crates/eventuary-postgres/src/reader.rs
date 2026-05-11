use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use chrono::{DateTime, Utc};
use either::Either;
use futures::Stream;
use sqlx::{PgPool, Row};
use tokio::sync::mpsc;

use eventuary_core::io::acker::{NoopAcker, OnceAcker};
use eventuary_core::io::{Acker, Message, Reader};
use eventuary_core::{
    ConsumerGroupId, Error, EventSubscription, Namespace, OrganizationId, Result, SerializedEvent,
    StartFrom, Topic,
};

const DEFAULT_STREAM: &str = "default";

#[derive(Clone)]
pub struct PgReaderConfig {
    pub organization: OrganizationId,
    pub namespace: Option<Namespace>,
    pub topics: Vec<Topic>,
    pub consumer_group_id: Option<ConsumerGroupId>,
    pub stream: String,
    pub start_from: StartFrom,
    pub poll_interval: Duration,
    pub batch_size: usize,
}

impl PgReaderConfig {
    pub fn new(organization: OrganizationId) -> Self {
        Self {
            organization,
            namespace: None,
            topics: Vec::new(),
            consumer_group_id: None,
            stream: DEFAULT_STREAM.to_owned(),
            start_from: StartFrom::Latest,
            poll_interval: Duration::from_millis(100),
            batch_size: 100,
        }
    }
}

/// ack advances the consumer group's checkpoint to this event's sequence;
/// nack leaves the checkpoint unchanged. Backwards moves are guarded by
/// `WHERE EXCLUDED.sequence > consumer_offsets.sequence`.
#[derive(Clone)]
pub struct PgAcker {
    pool: PgPool,
    organization: OrganizationId,
    consumer_group_id: ConsumerGroupId,
    stream: String,
    sequence: i64,
}

impl Acker for PgAcker {
    async fn ack(&self) -> Result<()> {
        sqlx::query(
            "INSERT INTO consumer_offsets (organization, consumer_group_id, stream, sequence) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (organization, consumer_group_id, stream) DO UPDATE \
             SET sequence = EXCLUDED.sequence \
             WHERE EXCLUDED.sequence > consumer_offsets.sequence",
        )
        .bind(self.organization.as_str())
        .bind(self.consumer_group_id.as_str())
        .bind(&self.stream)
        .bind(self.sequence)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::Store(e.to_string()))?;
        Ok(())
    }

    async fn nack(&self) -> Result<()> {
        Ok(())
    }
}

pub type PgAckerVariant = Either<NoopAcker, OnceAcker<PgAcker>>;

pub struct PgStream {
    rx: mpsc::Receiver<Result<Message<PgAckerVariant>>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for PgStream {
    fn drop(&mut self) {
        if let Some(h) = self.handle.take() {
            h.abort();
        }
    }
}

impl Stream for PgStream {
    type Item = Result<Message<PgAckerVariant>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

pub struct PgReader {
    pool: PgPool,
    config: PgReaderConfig,
}

impl PgReader {
    pub fn new(pool: PgPool, config: PgReaderConfig) -> Self {
        Self { pool, config }
    }

    pub async fn read(&self) -> Result<PgStream> {
        eventuary_core::io::Reader::read(self, subscription_from_config(&self.config)).await
    }
}

fn subscription_from_config(config: &PgReaderConfig) -> EventSubscription {
    let mut subscription = EventSubscription::new(config.organization.clone());
    subscription.name = Some(config.stream.clone());
    subscription.consumer_group_id = config.consumer_group_id.clone();
    if !config.topics.is_empty() {
        subscription.topics = Some(config.topics.clone());
    }
    subscription.namespace_prefix = config.namespace.clone();
    subscription.start_from = config.start_from;
    subscription
}

fn apply_subscription(config: &mut PgReaderConfig, subscription: &EventSubscription) {
    config.organization = subscription.organization.clone();
    config.namespace = subscription.namespace_prefix.clone();
    config.topics = subscription.topics.clone().unwrap_or_default();
    config.consumer_group_id = subscription
        .consumer_group_id
        .clone()
        .or_else(|| config.consumer_group_id.clone());
    if let Some(name) = subscription.name.as_ref() {
        config.stream = name.clone();
    }
    config.start_from = subscription.start_from;
}

impl Reader for PgReader {
    type Subscription = EventSubscription;
    type Acker = PgAckerVariant;
    type Stream = PgStream;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let pool = self.pool.clone();
        let mut config = self.config.clone();
        apply_subscription(&mut config, &subscription);
        let (tx, rx) = mpsc::channel(64);

        let handle = tokio::spawn(async move {
            let (mut after_seq, lower_bound_ts) =
                match resolve_initial_position(&pool, &config).await {
                    Ok(p) => p,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };

            let mut delivered = 0usize;
            loop {
                let take = config.batch_size.clamp(1, 1000);
                let batch = match fetch_batch(&pool, &config, after_seq, take, lower_bound_ts).await
                {
                    Ok(b) => b,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };

                tracing::trace!(after_seq, fetched = batch.len(), "postgres poll");

                if batch.is_empty() {
                    tokio::time::sleep(config.poll_interval).await;
                    continue;
                }

                for (serialized, sequence) in batch {
                    let event = match serialized.to_event() {
                        Ok(e) => e,
                        Err(e) => {
                            let _ = tx
                                .send(Err(Error::Serialization(format!(
                                    "decode event at sequence {sequence}: {e}"
                                ))))
                                .await;
                            return;
                        }
                    };
                    after_seq = sequence;
                    if !subscription.matches(&event) {
                        continue;
                    }
                    if let Some(limit) = subscription.limit
                        && delivered >= limit
                    {
                        return;
                    }
                    let acker: PgAckerVariant = match config.consumer_group_id.as_ref() {
                        Some(group) => Either::Right(OnceAcker::new(PgAcker {
                            pool: pool.clone(),
                            organization: config.organization.clone(),
                            consumer_group_id: group.clone(),
                            stream: config.stream.clone(),
                            sequence,
                        })),
                        None => Either::Left(NoopAcker),
                    };
                    if tx.send(Ok(Message::new(event, acker))).await.is_err() {
                        return;
                    }
                    delivered += 1;
                }
            }
        });

        Ok(PgStream {
            rx,
            handle: Some(handle),
        })
    }
}

async fn resolve_initial_position(
    pool: &PgPool,
    config: &PgReaderConfig,
) -> Result<(i64, Option<DateTime<Utc>>)> {
    if let Some(group) = config.consumer_group_id.as_ref() {
        let row = sqlx::query(
            "SELECT sequence FROM consumer_offsets \
             WHERE organization = $1 AND consumer_group_id = $2 AND stream = $3",
        )
        .bind(config.organization.as_str())
        .bind(group.as_str())
        .bind(&config.stream)
        .fetch_optional(pool)
        .await
        .map_err(|e| Error::Store(e.to_string()))?;
        if let Some(r) = row {
            return Ok((r.get::<i64, _>("sequence"), None));
        }
    }
    match config.start_from {
        StartFrom::Earliest => Ok((0, None)),
        StartFrom::Latest => {
            let row = sqlx::query(
                "SELECT COALESCE(MAX(sequence), 0) AS s FROM events WHERE organization = $1",
            )
            .bind(config.organization.as_str())
            .fetch_one(pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
            Ok((row.get::<i64, _>("s"), None))
        }
        StartFrom::Timestamp(ts) => {
            let row = sqlx::query(
                "SELECT COALESCE(MIN(sequence), 1) - 1 AS s FROM events \
                 WHERE organization = $1 AND timestamp >= $2::timestamptz",
            )
            .bind(config.organization.as_str())
            .bind(ts.to_rfc3339())
            .fetch_one(pool)
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
            Ok((row.get::<i64, _>("s").max(0), Some(ts)))
        }
    }
}

async fn fetch_batch(
    pool: &PgPool,
    config: &PgReaderConfig,
    after_seq: i64,
    take: usize,
    lower_bound_ts: Option<DateTime<Utc>>,
) -> Result<Vec<(SerializedEvent, i64)>> {
    let mut sql = String::from(
        "SELECT sequence, id::text AS id_text, organization, namespace, topic, event_key, \
         payload::text AS payload_text, content_type, metadata::text AS metadata_text, \
         timestamp::text AS timestamp_text, version, parent_id::text AS parent_id_text, \
         correlation_id, causation_id \
         FROM events WHERE sequence > $1 AND organization = $2",
    );
    let mut bind_index = 3usize;

    let topics_filter = !config.topics.is_empty();
    if topics_filter {
        sql.push_str(&format!(" AND topic = ANY(${bind_index})"));
        bind_index += 1;
    }
    let ns_filter = config
        .namespace
        .as_ref()
        .filter(|n| !n.is_root())
        .map(|n| n.as_str().to_owned());
    if ns_filter.is_some() {
        sql.push_str(&format!(
            " AND (namespace = ${bind_index} OR namespace LIKE ${bind_index} || '/%')"
        ));
        bind_index += 1;
    }
    if lower_bound_ts.is_some() {
        sql.push_str(&format!(" AND timestamp >= ${bind_index}::timestamptz"));
        bind_index += 1;
    }
    sql.push_str(&format!(" ORDER BY sequence ASC LIMIT ${bind_index}"));

    let mut q = sqlx::query(&sql)
        .bind(after_seq)
        .bind(config.organization.as_str());
    if topics_filter {
        let topics: Vec<String> = config
            .topics
            .iter()
            .map(|t| t.as_str().to_owned())
            .collect();
        q = q.bind(topics);
    }
    if let Some(prefix) = ns_filter {
        q = q.bind(prefix);
    }
    if let Some(ts) = lower_bound_ts {
        q = q.bind(ts.to_rfc3339());
    }
    q = q.bind(take as i64);

    let rows = q
        .fetch_all(pool)
        .await
        .map_err(|e| Error::Store(e.to_string()))?;

    rows.into_iter()
        .map(|row| {
            let sequence: i64 = row.get("sequence");
            let id: String = row.get("id_text");
            let payload_str: String = row.get("payload_text");
            let payload: serde_json::Value = serde_json::from_str(&payload_str)
                .map_err(|e| Error::Serialization(format!("decode payload: {e}")))?;
            let metadata_str: String = row.get("metadata_text");
            let metadata: HashMap<String, String> = serde_json::from_str(&metadata_str)
                .map_err(|e| Error::Serialization(format!("decode metadata: {e}")))?;
            let timestamp_str: String = row.get("timestamp_text");
            let timestamp = parse_pg_timestamp(&timestamp_str).map_err(|e| {
                Error::Serialization(format!("decode timestamp at sequence {sequence}: {e}"))
            })?;
            let serialized = SerializedEvent {
                id,
                organization: row.get("organization"),
                namespace: row.get("namespace"),
                topic: row.get("topic"),
                key: row.get("event_key"),
                payload,
                content_type: row.get("content_type"),
                metadata,
                timestamp,
                version: row.get::<i64, _>("version") as u64,
                parent_id: row.get("parent_id_text"),
                correlation_id: row.get("correlation_id"),
                causation_id: row.get("causation_id"),
            };
            Ok((serialized, sequence))
        })
        .collect()
}

fn parse_pg_timestamp(s: &str) -> std::result::Result<DateTime<Utc>, chrono::ParseError> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc));
    }
    DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%#z").map(|dt| dt.with_timezone(&Utc))
}
