use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::Stream;
use rusqlite::types::Value;
use tokio::sync::Mutex;
use tokio::sync::mpsc;

use eventuary_core::io::{Acker, EventFilter, Message, Reader};
use eventuary_core::{
    CursorPartition, Error, LogicalPartition, Result, SerializedEvent, StartFrom,
    StartableSubscription, TopicPattern,
};

use crate::database::SqliteConn;
use crate::relation::SqliteRelationName;

#[derive(
    Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct SqliteCursor {
    pub sequence: i64,
}

impl SqliteCursor {
    pub fn new(sequence: i64) -> Self {
        Self { sequence }
    }

    pub fn sequence(&self) -> i64 {
        self.sequence
    }
}

impl CursorPartition for SqliteCursor {
    fn partition(&self) -> Option<LogicalPartition> {
        None
    }
}

#[derive(Debug, Clone)]
pub struct SqliteSubscription {
    pub start: StartFrom<SqliteCursor>,
    pub filter: EventFilter,
    pub batch_size: Option<usize>,
    pub limit: Option<usize>,
}

impl Default for SqliteSubscription {
    fn default() -> Self {
        Self {
            start: StartFrom::Latest,
            filter: EventFilter::default(),
            batch_size: None,
            limit: None,
        }
    }
}

impl StartableSubscription<SqliteCursor> for SqliteSubscription {
    fn start(&self) -> &StartFrom<SqliteCursor> {
        &self.start
    }

    fn with_start(mut self, start: StartFrom<SqliteCursor>) -> Self {
        self.start = start;
        self
    }
}

#[derive(Debug, Clone)]
pub struct SqliteReaderConfig {
    pub events_relation: SqliteRelationName,
    pub poll_interval: Duration,
    pub default_batch_size: usize,
}

impl Default for SqliteReaderConfig {
    fn default() -> Self {
        Self {
            events_relation: SqliteRelationName::new("events").expect("default events relation"),
            poll_interval: Duration::from_millis(100),
            default_batch_size: 100,
        }
    }
}

#[derive(Clone)]
pub struct SqliteCursorAcker {
    state: Arc<Mutex<CursorState>>,
    sequence: i64,
}

struct CursorState {
    last_acked: i64,
    pending_nack: bool,
}

impl Acker for SqliteCursorAcker {
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

pub struct SqliteStream {
    rx: mpsc::Receiver<Result<Message<SqliteCursorAcker, SqliteCursor>>>,
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
    type Item = Result<Message<SqliteCursorAcker, SqliteCursor>>;

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
}

impl Reader for SqliteReader {
    type Subscription = SqliteSubscription;
    type Acker = SqliteCursorAcker;
    type Cursor = SqliteCursor;
    type Stream = SqliteStream;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let conn = Arc::clone(&self.conn);
        let events_relation = self.config.events_relation.render();
        let poll_interval = self.config.poll_interval;
        let batch_size = subscription
            .batch_size
            .unwrap_or(self.config.default_batch_size)
            .clamp(1, 1000);
        let filter = subscription.filter.clone();
        let limit = subscription.limit;
        let (tx, rx) = mpsc::channel(64);

        let (mut after_seq, lower_bound_ts) =
            match resolve_initial_position(&conn, &events_relation, &subscription).await {
                Ok(pos) => pos,
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return Ok(SqliteStream { rx, handle: None });
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
                        &conn,
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
                    let acker = SqliteCursorAcker {
                        state: Arc::clone(&state),
                        sequence,
                    };
                    let cursor = SqliteCursor { sequence };
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

        Ok(SqliteStream {
            rx,
            handle: Some(handle),
        })
    }
}

async fn resolve_initial_position(
    conn: &SqliteConn,
    events_relation: &str,
    subscription: &SqliteSubscription,
) -> Result<(i64, Option<DateTime<Utc>>)> {
    match subscription.start.clone() {
        StartFrom::After(cursor) => Ok((cursor.sequence, None)),
        StartFrom::Earliest => Ok((0, None)),
        StartFrom::Latest => {
            let conn = Arc::clone(conn);
            let org = subscription
                .filter
                .organization
                .as_ref()
                .map(|o| o.as_str().to_owned());
            let relation = events_relation.to_owned();
            tokio::task::spawn_blocking(move || {
                let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
                let seq: i64 = match org {
                    Some(o) => guard
                        .query_row(
                            &format!(
                                "SELECT COALESCE(MAX(sequence), 0) FROM {relation} WHERE organization = ?1"
                            ),
                            rusqlite::params![o],
                            |r| r.get(0),
                        )
                        .map_err(|e| Error::Store(e.to_string()))?,
                    None => guard
                        .query_row(
                            &format!("SELECT COALESCE(MAX(sequence), 0) FROM {relation}"),
                            [],
                            |r| r.get(0),
                        )
                        .map_err(|e| Error::Store(e.to_string()))?,
                };
                Ok::<(i64, Option<DateTime<Utc>>), Error>((seq, None))
            })
            .await
            .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
        }
        StartFrom::Timestamp(ts) => {
            let conn = Arc::clone(conn);
            let org = subscription
                .filter
                .organization
                .as_ref()
                .map(|o| o.as_str().to_owned());
            let ts_str = ts.to_rfc3339();
            let relation = events_relation.to_owned();
            tokio::task::spawn_blocking(move || {
                let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
                let seq: i64 = match org {
                    Some(o) => guard
                        .query_row(
                            &format!(
                                "SELECT COALESCE(MIN(sequence), 1) - 1 FROM {relation} \
                                 WHERE organization = ?1 AND timestamp >= ?2"
                            ),
                            rusqlite::params![o, ts_str],
                            |r| r.get(0),
                        )
                        .map_err(|e| Error::Store(e.to_string()))?,
                    None => guard
                        .query_row(
                            &format!(
                                "SELECT COALESCE(MIN(sequence), 1) - 1 FROM {relation} \
                                 WHERE timestamp >= ?1"
                            ),
                            rusqlite::params![ts_str],
                            |r| r.get(0),
                        )
                        .map_err(|e| Error::Store(e.to_string()))?,
                };
                Ok::<(i64, Option<DateTime<Utc>>), Error>((seq.max(0), Some(ts)))
            })
            .await
            .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
        }
    }
}

async fn fetch_batch(
    conn: &SqliteConn,
    events_relation: &str,
    after_seq: i64,
    take: usize,
    lower_bound_ts: Option<DateTime<Utc>>,
    filter: &EventFilter,
) -> Result<Vec<(SerializedEvent, i64)>> {
    let conn = Arc::clone(conn);
    let relation = events_relation.to_owned();
    let org = filter.organization.as_ref().map(|o| o.as_str().to_owned());
    let exact_topics: Vec<String> = filter
        .topics
        .as_ref()
        .map(|patterns| {
            patterns
                .iter()
                .map(|p| match p {
                    TopicPattern::Exact(t) => t.as_str().to_owned(),
                })
                .collect()
        })
        .unwrap_or_default();
    let ns_prefix = filter.namespace.as_ref().and_then(|p| match p {
        eventuary_core::NamespacePattern::Prefix(ns) if !ns.is_root() => {
            Some(ns.as_str().to_owned())
        }
        _ => None,
    });
    let ts_str = lower_bound_ts.map(|t| t.to_rfc3339());

    tokio::task::spawn_blocking(move || {
        let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;

        let mut sql = format!(
            "SELECT sequence, id, organization, namespace, topic, event_key, payload, content_type, metadata, \
             timestamp, version, parent_id, correlation_id, causation_id \
             FROM {relation} WHERE sequence > ?1"
        );
        let mut params: Vec<Value> = vec![Value::Integer(after_seq)];
        let mut idx = 2usize;

        if let Some(o) = &org {
            sql.push_str(&format!(" AND organization = ?{idx}"));
            params.push(Value::Text(o.clone()));
            idx += 1;
        }
        if !exact_topics.is_empty() {
            let placeholders: Vec<String> = exact_topics
                .iter()
                .enumerate()
                .map(|(i, _)| format!("?{}", idx + i))
                .collect();
            sql.push_str(&format!(" AND topic IN ({})", placeholders.join(",")));
            for t in &exact_topics {
                params.push(Value::Text(t.clone()));
            }
            idx += exact_topics.len();
        }
        if let Some(prefix) = &ns_prefix {
            sql.push_str(&format!(
                " AND (namespace = ?{idx} OR namespace LIKE ?{} || '/%')",
                idx
            ));
            params.push(Value::Text(prefix.clone()));
            idx += 1;
        }
        if let Some(ts) = &ts_str {
            sql.push_str(&format!(" AND timestamp >= ?{idx}"));
            params.push(Value::Text(ts.clone()));
            idx += 1;
        }
        sql.push_str(&format!(" ORDER BY sequence ASC LIMIT ?{idx}"));
        params.push(Value::Integer(take as i64));

        let mut stmt = guard
            .prepare(&sql)
            .map_err(|e| Error::Store(e.to_string()))?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params.iter()), |row| {
                let sequence: i64 = row.get(0)?;
                let id: String = row.get(1)?;
                let organization: String = row.get(2)?;
                let namespace: String = row.get(3)?;
                let topic: String = row.get(4)?;
                let key: Option<String> = row.get(5)?;
                let payload_str: String = row.get(6)?;
                let content_type: String = row.get(7)?;
                let metadata_str: String = row.get(8)?;
                let timestamp_str: String = row.get(9)?;
                let version: i64 = row.get(10)?;
                let parent_id: Option<String> = row.get(11)?;
                let correlation_id: Option<String> = row.get(12)?;
                let causation_id: Option<String> = row.get(13)?;
                Ok((
                    sequence,
                    id,
                    organization,
                    namespace,
                    topic,
                    key,
                    payload_str,
                    content_type,
                    metadata_str,
                    timestamp_str,
                    version,
                    parent_id,
                    correlation_id,
                    causation_id,
                ))
            })
            .map_err(|e| Error::Store(e.to_string()))?;

        let mut out = Vec::new();
        for row in rows {
            let (
                sequence,
                id,
                organization,
                namespace,
                topic,
                key,
                payload_str,
                content_type,
                metadata_str,
                timestamp_str,
                version,
                parent_id,
                correlation_id,
                causation_id,
            ) = row.map_err(|e| Error::Store(e.to_string()))?;

            let payload: serde_json::Value = serde_json::from_str(&payload_str)
                .map_err(|e| Error::Serialization(format!("decode payload: {e}")))?;
            let metadata: HashMap<String, String> = serde_json::from_str(&metadata_str)
                .map_err(|e| Error::Serialization(format!("decode metadata: {e}")))?;
            let timestamp = DateTime::parse_from_rfc3339(&timestamp_str)
                .map(|d| d.with_timezone(&Utc))
                .map_err(|e| Error::Serialization(format!("decode timestamp: {e}")))?;
            out.push((
                SerializedEvent {
                    id,
                    organization,
                    namespace,
                    topic,
                    key,
                    payload,
                    content_type,
                    metadata,
                    timestamp,
                    version: version as u64,
                    parent_id,
                    correlation_id,
                    causation_id,
                },
                sequence,
            ));
        }
        Ok(out)
    })
    .await
    .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
}
