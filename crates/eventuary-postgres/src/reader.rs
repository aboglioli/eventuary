use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::Stream;
use sqlx::{PgPool, Row};
use tokio::sync::Mutex;
use tokio::sync::mpsc;

use eventuary_core::io::{Acker, EventFilter, Message, Reader};
use eventuary_core::{
    CommitCursor, CursorPartition, Error, LogicalPartition, Result, SerializedEvent, StartFrom,
    StartableSubscription, TopicPattern,
};

use crate::relation::PgRelationName;

#[derive(
    Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct PgCursor {
    pub sequence: i64,
}

impl PgCursor {
    pub fn new(sequence: i64) -> Self {
        Self { sequence }
    }

    pub fn sequence(&self) -> i64 {
        self.sequence
    }
}

impl CursorPartition for PgCursor {
    fn partition(&self) -> Option<LogicalPartition> {
        None
    }
}

impl CommitCursor for PgCursor {
    type Commit = PgCursor;
    fn commit_cursor(&self) -> Self::Commit {
        *self
    }
}

#[derive(Debug, Clone)]
pub struct PgSubscription {
    pub start: StartFrom<PgCursor>,
    pub filter: EventFilter,
    pub batch_size: Option<usize>,
    pub limit: Option<usize>,
}

impl Default for PgSubscription {
    fn default() -> Self {
        Self {
            start: StartFrom::Latest,
            filter: EventFilter::default(),
            batch_size: None,
            limit: None,
        }
    }
}

impl StartableSubscription<PgCursor> for PgSubscription {
    fn with_start(mut self, start: StartFrom<PgCursor>) -> Self {
        self.start = start;
        self
    }
}

#[derive(Debug, Clone)]
pub struct PgReaderConfig {
    pub events_relation: PgRelationName,
    pub poll_interval: Duration,
    pub default_batch_size: usize,
}

impl Default for PgReaderConfig {
    fn default() -> Self {
        Self {
            events_relation: PgRelationName::new("events").expect("default events relation"),
            poll_interval: Duration::from_millis(100),
            default_batch_size: 100,
        }
    }
}

/// Source-side acker. Holds shared cursor state so an unacked message is
/// re-emitted on the next stream poll instead of being dropped.
#[derive(Clone)]
pub struct PgCursorAcker {
    state: Arc<Mutex<CursorState>>,
    sequence: i64,
}

struct CursorState {
    last_acked: i64,
    pending_nack: bool,
}

impl Acker for PgCursorAcker {
    async fn ack(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        if self.sequence > state.last_acked {
            state.last_acked = self.sequence;
        }
        state.pending_nack = false;
        Ok(())
    }

    async fn nack(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        state.pending_nack = true;
        Ok(())
    }
}

pub struct PgStream {
    rx: mpsc::Receiver<Result<Message<PgCursorAcker, PgCursor>>>,
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
    type Item = Result<Message<PgCursorAcker, PgCursor>>;

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
}

impl Reader for PgReader {
    type Subscription = PgSubscription;
    type Acker = PgCursorAcker;
    type Cursor = PgCursor;
    type Stream = PgStream;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let pool = self.pool.clone();
        let config = self.config.clone();
        let (tx, rx) = mpsc::channel(64);
        let events_relation = config.events_relation.render();
        let poll_interval = config.poll_interval;
        let batch_size = subscription
            .batch_size
            .unwrap_or(config.default_batch_size)
            .clamp(1, 1000);
        let filter = subscription.filter.clone();
        let limit = subscription.limit;

        let (mut after_seq, lower_bound_ts) =
            match resolve_initial_position(&pool, &events_relation, &subscription).await {
                Ok(pos) => pos,
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return Ok(PgStream { rx, handle: None });
                }
            };

        let state = Arc::new(Mutex::new(CursorState {
            last_acked: after_seq,
            pending_nack: false,
        }));

        let handle = tokio::spawn(async move {
            let mut delivered = 0usize;
            let mut buffer: VecDeque<(SerializedEvent, i64)> = VecDeque::new();
            loop {
                if buffer.is_empty() {
                    let fetched = match fetch_batch(
                        &pool,
                        &events_relation,
                        after_seq,
                        batch_size,
                        lower_bound_ts,
                        &filter,
                    )
                    .await
                    {
                        Ok(b) => b,
                        Err(e) => {
                            let _ = tx.send(Err(e)).await;
                            return;
                        }
                    };
                    if fetched.is_empty() {
                        tokio::time::sleep(poll_interval).await;
                        continue;
                    }
                    buffer.extend(fetched);
                }

                while let Some((serialized, sequence)) = buffer.front() {
                    let sequence = *sequence;
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
                    if !filter.matches(&event) {
                        buffer.pop_front();
                        after_seq = sequence;
                        continue;
                    }
                    if let Some(l) = limit
                        && delivered >= l
                    {
                        return;
                    }
                    let acker = PgCursorAcker {
                        state: Arc::clone(&state),
                        sequence,
                    };
                    let cursor = PgCursor { sequence };
                    if tx
                        .send(Ok(Message::new(event, acker, cursor)))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    delivered += 1;

                    loop {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        let guard = state.lock().await;
                        if guard.last_acked >= sequence {
                            after_seq = sequence;
                            drop(guard);
                            buffer.pop_front();
                            break;
                        }
                        if guard.pending_nack {
                            drop(guard);
                            let mut g = state.lock().await;
                            g.pending_nack = false;
                            break;
                        }
                        if tx.is_closed() {
                            return;
                        }
                    }
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
    events_relation: &str,
    subscription: &PgSubscription,
) -> Result<(i64, Option<DateTime<Utc>>)> {
    match subscription.start.clone() {
        StartFrom::After(cursor) => Ok((cursor.sequence, None)),
        StartFrom::Earliest => Ok((0, None)),
        StartFrom::Latest => {
            let sql = match subscription.filter.organization.as_ref() {
                Some(_) => format!(
                    "SELECT COALESCE(MAX(sequence), 0) AS s FROM {events_relation} WHERE organization = $1",
                ),
                None => format!("SELECT COALESCE(MAX(sequence), 0) AS s FROM {events_relation}"),
            };
            let mut q = sqlx::query(&sql);
            if let Some(org) = subscription.filter.organization.as_ref() {
                q = q.bind(org.as_str());
            }
            let row = q
                .fetch_one(pool)
                .await
                .map_err(|e| Error::Store(e.to_string()))?;
            Ok((row.get::<i64, _>("s"), None))
        }
        StartFrom::Timestamp(ts) => {
            let sql = match subscription.filter.organization.as_ref() {
                Some(_) => format!(
                    "SELECT COALESCE(MIN(sequence), 1) - 1 AS s FROM {events_relation} \
                     WHERE organization = $1 AND timestamp >= $2::timestamptz",
                ),
                None => format!(
                    "SELECT COALESCE(MIN(sequence), 1) - 1 AS s FROM {events_relation} \
                     WHERE timestamp >= $1::timestamptz",
                ),
            };
            let mut q = sqlx::query(&sql);
            if let Some(org) = subscription.filter.organization.as_ref() {
                q = q.bind(org.as_str());
            }
            q = q.bind(ts.to_rfc3339());
            let row = q
                .fetch_one(pool)
                .await
                .map_err(|e| Error::Store(e.to_string()))?;
            Ok((row.get::<i64, _>("s").max(0), Some(ts)))
        }
    }
}

async fn fetch_batch(
    pool: &PgPool,
    events_relation: &str,
    after_seq: i64,
    take: usize,
    lower_bound_ts: Option<DateTime<Utc>>,
    filter: &EventFilter,
) -> Result<Vec<(SerializedEvent, i64)>> {
    let mut sql = format!(
        "SELECT sequence, id::text AS id_text, organization, namespace, topic, event_key, \
         payload::text AS payload_text, content_type, metadata::text AS metadata_text, \
         timestamp::text AS timestamp_text, version, parent_id::text AS parent_id_text, \
         correlation_id, causation_id \
         FROM {events_relation} WHERE sequence > $1",
    );
    let mut bind_index = 2usize;

    if filter.organization.is_some() {
        sql.push_str(&format!(" AND organization = ${bind_index}"));
        bind_index += 1;
    }

    let exact_topic: Option<String> = filter.topic.as_ref().map(|p| match p {
        TopicPattern::Exact(t) => t.as_str().to_owned(),
    });
    if exact_topic.is_some() {
        sql.push_str(&format!(" AND topic = ${bind_index}"));
        bind_index += 1;
    }
    let ns_filter = filter.namespace.as_ref().and_then(|p| match p {
        eventuary_core::NamespacePattern::Prefix(ns) if !ns.is_root() => {
            Some(ns.as_str().to_owned())
        }
        _ => None,
    });
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

    let mut q = sqlx::query(&sql).bind(after_seq);

    if let Some(org) = &filter.organization {
        q = q.bind(org.as_str());
    }
    if let Some(topic) = exact_topic {
        q = q.bind(topic);
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
