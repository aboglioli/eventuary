use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use chrono::{DateTime, Utc};
use either::Either;
use futures::Stream;
use tokio::sync::mpsc;

use eventuary_core::io::acker::{NoopAcker, OnceAcker};
use eventuary_core::io::{Acker, Message, Reader};
use eventuary_core::{
    ConsumerGroupId, Error, EventSubscription, Namespace, OrganizationId, Result, SerializedEvent,
    StartFrom, Topic,
};

use crate::database::SqliteConn;

const DEFAULT_CHECKPOINT_NAME: &str = "default";

#[derive(Clone)]
pub struct SqliteReaderConfig {
    pub organization: Option<OrganizationId>,
    pub namespace: Option<Namespace>,
    pub topics: Vec<Topic>,
    pub consumer_group_id: Option<ConsumerGroupId>,
    pub checkpoint_name: String,
    pub start_from: StartFrom,
    pub poll_interval: Duration,
    pub batch_size: usize,
}

impl Default for SqliteReaderConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl SqliteReaderConfig {
    pub fn new() -> Self {
        Self {
            organization: None,
            namespace: None,
            topics: Vec::new(),
            consumer_group_id: None,
            checkpoint_name: DEFAULT_CHECKPOINT_NAME.to_owned(),
            start_from: StartFrom::Latest,
            poll_interval: Duration::from_millis(100),
            batch_size: 100,
        }
    }

    pub fn for_organization(organization: OrganizationId) -> Self {
        Self {
            organization: Some(organization),
            ..Self::new()
        }
    }
}

/// ack advances the consumer group's checkpoint to this event's sequence;
/// nack leaves the checkpoint unchanged so the event is redelivered on next
/// reader start. Backwards moves are guarded by
/// `WHERE excluded.sequence > consumer_offsets.sequence`.
///
/// The (`partition`, `partition_count`) pair scopes the checkpoint row to a
/// specific partition assignment. Unpartitioned consumers use `(0, 1)`,
/// matching the column defaults, so they transparently share one row.
#[derive(Clone)]
pub struct SqliteAcker {
    conn: SqliteConn,
    consumer_group_id: ConsumerGroupId,
    checkpoint_name: String,
    partition: i32,
    partition_count: i32,
    sequence: i64,
}

impl Acker for SqliteAcker {
    async fn ack(&self) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        let consumer_group_id = self.consumer_group_id.as_str().to_owned();
        let checkpoint_name = self.checkpoint_name.clone();
        let partition = self.partition;
        let partition_count = self.partition_count;
        let sequence = self.sequence;
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            guard
                .execute(
                    "INSERT INTO consumer_offsets
                       (consumer_group_id, checkpoint_name, partition, partition_count, sequence)
                     VALUES (?1, ?2, ?3, ?4, ?5)
                     ON CONFLICT(consumer_group_id, checkpoint_name, partition, partition_count)
                     DO UPDATE SET sequence = excluded.sequence
                     WHERE excluded.sequence > consumer_offsets.sequence",
                    rusqlite::params![
                        consumer_group_id,
                        checkpoint_name,
                        partition,
                        partition_count,
                        sequence
                    ],
                )
                .map_err(|e| Error::Store(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
    }

    async fn nack(&self) -> Result<()> {
        Ok(())
    }
}

pub type SqliteAckerVariant = Either<NoopAcker, OnceAcker<SqliteAcker>>;

pub struct SqliteStream {
    rx: mpsc::Receiver<Result<Message<SqliteAckerVariant>>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for SqliteStream {
    fn drop(&mut self) {
        if let Some(h) = self.handle.take() {
            h.abort();
        }
    }
}

impl Stream for SqliteStream {
    type Item = Result<Message<SqliteAckerVariant>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

pub struct SqliteReader {
    conn: SqliteConn,
    config: SqliteReaderConfig,
}

impl SqliteReader {
    pub fn new(conn: SqliteConn, config: SqliteReaderConfig) -> Self {
        Self { conn, config }
    }

    pub async fn read(&self) -> Result<SqliteStream> {
        eventuary_core::io::Reader::read(self, subscription_from_config(&self.config)).await
    }
}

fn subscription_from_config(config: &SqliteReaderConfig) -> EventSubscription {
    let mut subscription = EventSubscription::new();
    subscription.organization = config.organization.clone();
    subscription.checkpoint_name = Some(config.checkpoint_name.clone());
    subscription.consumer_group_id = config.consumer_group_id.clone();
    if !config.topics.is_empty() {
        subscription.topics = Some(config.topics.clone());
    }
    subscription.namespace_prefix = config.namespace.clone();
    subscription.start_from = config.start_from;
    subscription
}

fn apply_subscription(config: &mut SqliteReaderConfig, subscription: &EventSubscription) {
    config.organization = subscription.organization.clone();
    config.namespace = subscription.namespace_prefix.clone();
    config.topics = subscription.topics.clone().unwrap_or_default();
    config.consumer_group_id = subscription
        .consumer_group_id
        .clone()
        .or_else(|| config.consumer_group_id.clone());
    if let Some(checkpoint_name) = subscription.checkpoint_name.as_ref() {
        config.checkpoint_name = checkpoint_name.clone();
    }
    config.start_from = subscription.start_from;
}

impl Reader for SqliteReader {
    type Subscription = EventSubscription;
    type Acker = SqliteAckerVariant;
    type Cursor = eventuary_core::io::NoCursor;
    type Stream = SqliteStream;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let conn = Arc::clone(&self.conn);
        let mut config = self.config.clone();
        apply_subscription(&mut config, &subscription);
        if subscription.partition.is_some() && config.consumer_group_id.is_none() {
            return Err(Error::Config(
                "subscription.partition requires consumer_group_id; partition checkpoints have no identity without a group".to_owned(),
            ));
        }
        let (partition_id, partition_count) = match subscription.partition {
            Some(a) => (a.id() as i32, a.count() as i32),
            None => (0, 1),
        };
        tracing::info!(
            consumer_group_id = config.consumer_group_id.as_ref().map(|g| g.as_str()),
            checkpoint_name = %config.checkpoint_name,
            partition = partition_id,
            partition_count = partition_count,
            "sqlite reader spawned",
        );
        let (tx, rx) = mpsc::channel(64);

        let handle = tokio::spawn(async move {
            let initial = {
                let conn = Arc::clone(&conn);
                let config = config.clone();
                tokio::task::spawn_blocking(move || {
                    resolve_initial_position(&conn, &config, partition_id, partition_count)
                })
                .await
            };
            let (mut after_seq, lower_bound_ts) = match initial {
                Ok(Ok(p)) => p,
                Ok(Err(e)) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
                Err(e) => {
                    let _ = tx
                        .send(Err(Error::Store(format!("blocking task panicked: {e}"))))
                        .await;
                    return;
                }
            };

            let mut delivered = 0usize;
            loop {
                let take = config.batch_size.clamp(1, 1000);
                let fetch = {
                    let conn = Arc::clone(&conn);
                    let config = config.clone();
                    tokio::task::spawn_blocking(move || {
                        fetch_batch(&conn, &config, after_seq, take, lower_bound_ts)
                    })
                    .await
                };
                let batch = match fetch {
                    Ok(Ok(b)) => b,
                    Ok(Err(e)) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                    Err(e) => {
                        let _ = tx
                            .send(Err(Error::Store(format!("blocking task panicked: {e}"))))
                            .await;
                        return;
                    }
                };

                tracing::trace!(
                    after_seq,
                    fetched = batch.len(),
                    partition = partition_id,
                    partition_count = partition_count,
                    "sqlite poll",
                );

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
                    let acker: SqliteAckerVariant = match config.consumer_group_id.as_ref() {
                        Some(group) => Either::Right(OnceAcker::new(SqliteAcker {
                            conn: Arc::clone(&conn),
                            consumer_group_id: group.clone(),
                            checkpoint_name: config.checkpoint_name.clone(),
                            partition: partition_id,
                            partition_count,
                            sequence,
                        })),
                        None => Either::Left(NoopAcker),
                    };
                    if tx
                        .send(Ok(Message::new(event, acker, eventuary_core::io::NoCursor)))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    delivered += 1;
                }
            }
        });

        Ok(SqliteStream {
            rx,
            handle: Some(handle),
        })
    }
}

fn resolve_initial_position(
    conn: &SqliteConn,
    config: &SqliteReaderConfig,
    partition: i32,
    partition_count: i32,
) -> Result<(i64, Option<DateTime<Utc>>)> {
    let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
    if let Some(group) = config.consumer_group_id.as_ref() {
        let row: Option<i64> = guard
            .query_row(
                "SELECT sequence FROM consumer_offsets
                 WHERE consumer_group_id = ?1
                   AND checkpoint_name   = ?2
                   AND partition         = ?3
                   AND partition_count   = ?4",
                rusqlite::params![
                    group.as_str(),
                    config.checkpoint_name,
                    partition,
                    partition_count
                ],
                |r| r.get(0),
            )
            .ok();
        if let Some(s) = row {
            return Ok((s, None));
        }
    }
    match config.start_from {
        StartFrom::Earliest => Ok((0, None)),
        StartFrom::Latest => {
            let sql = match config.organization.as_ref() {
                Some(org) => {
                    let sql =
                        "SELECT COALESCE(MAX(sequence), 0) FROM events WHERE organization = ?1";
                    guard
                        .query_row(sql, rusqlite::params![org.as_str()], |r| r.get::<_, i64>(0))
                        .map_err(|e| Error::Store(e.to_string()))?
                }
                None => {
                    let sql = "SELECT COALESCE(MAX(sequence), 0) FROM events";
                    guard
                        .query_row(sql, [], |r| r.get::<_, i64>(0))
                        .map_err(|e| Error::Store(e.to_string()))?
                }
            };
            Ok((sql, None))
        }
        StartFrom::Timestamp(ts) => {
            let row = match config.organization.as_ref() {
                Some(org) => {
                    let sql = "SELECT MIN(sequence) - 1 FROM events
                               WHERE organization = ?1 AND timestamp >= ?2";
                    guard
                        .query_row(sql, rusqlite::params![org.as_str(), ts.to_rfc3339()], |r| {
                            r.get(0)
                        })
                        .ok()
                }
                None => {
                    let sql = "SELECT MIN(sequence) - 1 FROM events
                               WHERE timestamp >= ?1";
                    guard
                        .query_row(sql, rusqlite::params![ts.to_rfc3339()], |r| r.get(0))
                        .ok()
                }
            };
            Ok((row.unwrap_or(0).max(0), Some(ts)))
        }
    }
}

fn fetch_batch(
    conn: &SqliteConn,
    config: &SqliteReaderConfig,
    after_seq: i64,
    take: usize,
    lower_bound_ts: Option<DateTime<Utc>>,
) -> Result<Vec<(SerializedEvent, i64)>> {
    let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;

    let mut sql = String::from(
        "SELECT sequence, id, organization, namespace, topic, event_key, payload, content_type, metadata, timestamp, version, parent_id, correlation_id, causation_id \
         FROM events WHERE sequence > ?1",
    );
    let mut bindings: Vec<rusqlite::types::Value> = vec![after_seq.into()];

    if let Some(org) = config.organization.as_ref() {
        let i = bindings.len() + 1;
        sql.push_str(&format!(" AND organization = ?{i}"));
        bindings.push(org.as_str().to_owned().into());
    }

    if !config.topics.is_empty() {
        let placeholders: Vec<String> = (0..config.topics.len())
            .map(|i| format!("?{}", bindings.len() + 1 + i))
            .collect();
        sql.push_str(&format!(" AND topic IN ({})", placeholders.join(", ")));
        for t in &config.topics {
            bindings.push(t.as_str().to_owned().into());
        }
    }

    if let Some(prefix) = config.namespace.as_ref()
        && !prefix.is_root()
    {
        let i1 = bindings.len() + 1;
        let i2 = bindings.len() + 2;
        sql.push_str(&format!(" AND (namespace = ?{i1} OR namespace LIKE ?{i2})"));
        bindings.push(prefix.as_str().to_owned().into());
        bindings.push(format!("{}/%", prefix.as_str()).into());
    }

    if let Some(ts) = lower_bound_ts {
        let i = bindings.len() + 1;
        sql.push_str(&format!(" AND timestamp >= ?{i}"));
        bindings.push(ts.to_rfc3339().into());
    }

    let i = bindings.len() + 1;
    sql.push_str(&format!(" ORDER BY sequence ASC LIMIT ?{i}"));
    bindings.push((take as i64).into());

    let mut stmt = guard
        .prepare(&sql)
        .map_err(|e| Error::Store(e.to_string()))?;
    let params: Vec<&dyn rusqlite::ToSql> =
        bindings.iter().map(|v| v as &dyn rusqlite::ToSql).collect();
    let rows = stmt
        .query_map(&*params, |row| {
            let sequence: i64 = row.get(0)?;
            let payload_str: String = row.get(6)?;
            let metadata_str: String = row.get(8)?;
            let timestamp_str: String = row.get(9)?;
            Ok(RawRow {
                sequence,
                id: row.get(1)?,
                organization: row.get(2)?,
                namespace: row.get(3)?,
                topic: row.get(4)?,
                event_key: row.get(5)?,
                payload_str,
                content_type: row.get(7)?,
                metadata_str,
                timestamp_str,
                version: row.get::<_, i64>(10)? as u64,
                parent_id: row.get(11)?,
                correlation_id: row.get(12)?,
                causation_id: row.get(13)?,
            })
        })
        .map_err(|e| Error::Store(e.to_string()))?;

    let mut out = Vec::new();
    for r in rows {
        let raw = r.map_err(|e| Error::Store(e.to_string()))?;
        let sequence = raw.sequence;
        let serialized = raw.into_serialized()?;
        out.push((serialized, sequence));
    }
    Ok(out)
}

struct RawRow {
    sequence: i64,
    id: String,
    organization: String,
    namespace: String,
    topic: String,
    event_key: Option<String>,
    payload_str: String,
    content_type: String,
    metadata_str: String,
    timestamp_str: String,
    version: u64,
    parent_id: Option<String>,
    correlation_id: Option<String>,
    causation_id: Option<String>,
}

impl RawRow {
    fn into_serialized(self) -> Result<SerializedEvent> {
        let payload: serde_json::Value = serde_json::from_str(&self.payload_str)
            .map_err(|e| Error::Serialization(format!("decode payload: {e}")))?;
        let metadata: std::collections::HashMap<String, String> =
            serde_json::from_str(&self.metadata_str)
                .map_err(|e| Error::Serialization(format!("decode metadata: {e}")))?;
        let timestamp = DateTime::parse_from_rfc3339(&self.timestamp_str)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(|e| {
                Error::Serialization(format!(
                    "decode timestamp at sequence {}: {e}",
                    self.sequence
                ))
            })?;
        Ok(SerializedEvent {
            id: self.id,
            organization: self.organization,
            namespace: self.namespace,
            topic: self.topic,
            key: self.event_key,
            payload,
            content_type: self.content_type,
            metadata,
            timestamp,
            version: self.version,
            parent_id: self.parent_id,
            correlation_id: self.correlation_id,
            causation_id: self.causation_id,
        })
    }
}
