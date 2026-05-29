use std::collections::{HashMap, VecDeque};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use rusqlite::types::Value;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::sync::mpsc;

use eventuary_core::io::cursor::{CursorOrder, JsonCursorCodec};
use eventuary_core::io::filter::{EventFilter, NamespacePattern, TopicPattern};
use eventuary_core::io::reader::{
    CoordinatedAcker, CoordinatedCursor, CoordinatedReader, CoordinatedReaderConfig,
    CoordinatedStream, CoordinatedSubscription, PartitionAcker, PartitionedCoordAdapter,
    PartitionedCursor,
};
use eventuary_core::io::stream::SpawnedStream;
use eventuary_core::io::{Acker, Cursor, Filter, Message, Reader};
use eventuary_core::partition::{HasPartition, Partition, PartitionGroup, PartitionSelection};
use eventuary_core::{
    Error, PartitionableSubscription, Result, SerializedEvent, SerializedPayload, StartFrom,
    StartableSubscription, StopAt,
};

use crate::coordinator::SqlitePartitionCoordinator;

use crate::database::SqliteConn;
use crate::event_log::{SqliteEventLogSchema, SqliteEventLogSchemaConfig};
use crate::relation::SqliteRelationName;

#[derive(
    Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct SqliteCursor {
    pub sequence: i64,
    pub partition: Partition,
}

impl SqliteCursor {
    pub fn new(sequence: i64, partition: Partition) -> Self {
        Self {
            sequence,
            partition,
        }
    }

    pub fn sequence(&self) -> i64 {
        self.sequence
    }

    pub fn partition(&self) -> Partition {
        self.partition
    }
}

impl Cursor for SqliteCursor {
    fn order_key(&self) -> CursorOrder {
        CursorOrder::from_i64(self.sequence)
    }
}

impl HasPartition for SqliteCursor {
    fn partition(&self) -> Partition {
        self.partition
    }
}

impl SqliteCursor {
    pub fn codec() -> Result<JsonCursorCodec<Self>> {
        JsonCursorCodec::new("eventuary.sqlite.sqlite_cursor.v1")
    }
}

#[derive(Debug, Clone)]
pub struct SqliteSubscription {
    pub start: StartFrom<SqliteCursor>,
    pub stop_at: StopAt<SqliteCursor>,
    pub filter: EventFilter,
    pub partitions: PartitionSelection,
    pub batch_size: Option<usize>,
    pub limit: Option<usize>,
}

impl Default for SqliteSubscription {
    fn default() -> Self {
        Self {
            start: StartFrom::Latest,
            stop_at: StopAt::Never,
            filter: EventFilter::default(),
            batch_size: None,
            limit: None,
            partitions: PartitionSelection::default(),
        }
    }
}

impl StartableSubscription<SqliteCursor> for SqliteSubscription {
    fn with_start(mut self, start: StartFrom<SqliteCursor>) -> Self {
        self.start = start;
        self
    }
}

impl PartitionableSubscription<SqliteCursor> for SqliteSubscription {
    /// Restrict this subscription to a validated group of partitions sharing
    /// the same `partition_count`. The reader emits a single SQL query per
    /// poll using `partition_id IN (?, ?, ...)` instead of one query per
    /// partition. Single-partition uses fall through the default trait impl
    /// which wraps in a singleton group; `IN (?)` plans identically to `= ?`
    /// in SQLite.
    fn with_partitions(mut self, group: PartitionGroup) -> Self {
        self.partitions = PartitionSelection::Many(group);
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
    notify: Arc<Notify>,
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
        self.notify.notify_waiters();
        Ok(())
    }

    async fn nack(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        state.pending_nack = true;
        self.notify.notify_waiters();
        Ok(())
    }
}

pub struct SqliteReader {
    conn: SqliteConn,
    config: SqliteReaderConfig,
}

impl Clone for SqliteReader {
    fn clone(&self) -> Self {
        Self {
            conn: Arc::clone(&self.conn),
            config: self.config.clone(),
        }
    }
}

impl SqliteReader {
    pub fn connect(conn: SqliteConn, config: SqliteReaderConfig) -> Result<Self> {
        Self::prepare_schema(&conn, &config)?;
        Ok(Self::new(conn, config))
    }

    pub fn prepare_schema(conn: &SqliteConn, config: &SqliteReaderConfig) -> Result<()> {
        let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
        SqliteEventLogSchema::prepare(
            &guard,
            &SqliteEventLogSchemaConfig {
                events_relation: config.events_relation.clone(),
            },
        )
    }

    pub fn schema_sql(config: &SqliteReaderConfig) -> String {
        SqliteEventLogSchema::schema_sql(&SqliteEventLogSchemaConfig {
            events_relation: config.events_relation.clone(),
        })
    }

    pub fn new(conn: SqliteConn, config: SqliteReaderConfig) -> Self {
        Self { conn, config }
    }
}

impl Reader for SqliteReader {
    type Subscription = SqliteSubscription;
    type Acker = SqliteCursorAcker;
    type Cursor = SqliteCursor;
    type Stream = SpawnedStream<SqliteCursorAcker, SqliteCursor>;

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
        let partitions = subscription.partitions.clone();
        let (tx, rx) = mpsc::channel(64);

        let (mut after_seq, lower_bound_ts) =
            match resolve_initial_position(&conn, &events_relation, &subscription).await {
                Ok(pos) => pos,
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return Ok(SpawnedStream::from_receiver(rx));
                }
            };

        let stop_seq = match resolve_stop_position(&conn, &events_relation, &subscription).await {
            Ok(pos) => pos,
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                return Ok(SpawnedStream::from_receiver(rx));
            }
        };

        let state = Arc::new(Mutex::new(CursorState {
            last_acked: after_seq,
            pending_nack: false,
        }));
        let notify = Arc::new(Notify::new());

        let handle = tokio::spawn(async move {
            let mut delivered = 0usize;
            let mut buffer: VecDeque<(SerializedEvent, i64, Partition)> = VecDeque::new();
            loop {
                if buffer.is_empty() {
                    let fetched = match fetch_batch(
                        &conn,
                        FetchBatchParams {
                            events_relation: &events_relation,
                            after_seq,
                            stop_seq,
                            take: batch_size,
                            lower_bound_ts,
                            filter: &filter,
                            partitions: &partitions,
                        },
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
                        if stop_seq.is_some() {
                            return;
                        }
                        tokio::time::sleep(poll_interval).await;
                        continue;
                    }
                    buffer.extend(fetched);
                }

                while let Some((serialized, sequence, partition)) = buffer.front() {
                    let sequence = *sequence;
                    let partition = *partition;
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
                        notify: Arc::clone(&notify),
                        sequence,
                    };
                    let cursor = SqliteCursor {
                        sequence,
                        partition,
                    };
                    if tx
                        .send(Ok(Message::new(event, acker, cursor)))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    delivered += 1;

                    loop {
                        {
                            let guard = state.lock().await;
                            if guard.last_acked >= sequence {
                                after_seq = sequence;
                                buffer.pop_front();
                                break;
                            }
                            if guard.pending_nack {
                                break;
                            }
                            if tx.is_closed() {
                                return;
                            }
                        }
                        notify.notified().await;
                    }
                }
            }
        });

        Ok(SpawnedStream::new(rx, handle))
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

async fn resolve_stop_position(
    conn: &SqliteConn,
    events_relation: &str,
    subscription: &SqliteSubscription,
) -> Result<Option<i64>> {
    match subscription.stop_at {
        StopAt::Never => Ok(None),
        StopAt::Cursor(cursor) => Ok(Some(cursor.sequence)),
        StopAt::CurrentEnd => {
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
                Ok::<Option<i64>, Error>(Some(seq))
            })
            .await
            .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
        }
    }
}

struct FetchBatchParams<'a> {
    events_relation: &'a str,
    after_seq: i64,
    stop_seq: Option<i64>,
    take: usize,
    lower_bound_ts: Option<DateTime<Utc>>,
    filter: &'a EventFilter,
    partitions: &'a PartitionSelection,
}

async fn fetch_batch(
    conn: &SqliteConn,
    p: FetchBatchParams<'_>,
) -> Result<Vec<(SerializedEvent, i64, Partition)>> {
    let FetchBatchParams {
        events_relation,
        after_seq,
        stop_seq,
        take,
        lower_bound_ts,
        filter,
        partitions,
    } = p;
    let conn = Arc::clone(conn);
    let relation = events_relation.to_owned();
    let org = filter.organization.as_ref().map(|o| o.as_str().to_owned());
    let exact_topic: Option<String> = filter.topic.as_ref().map(|p| match p {
        TopicPattern::Exact(t) => t.as_str().to_owned(),
    });
    let ns_prefix = filter.namespace.as_ref().and_then(|p| match p {
        NamespacePattern::Prefix(ns) if !ns.is_root() => Some(ns.as_str().to_owned()),
        _ => None,
    });
    let ts_str = lower_bound_ts.map(|t| t.to_rfc3339());
    let partitions = partitions.clone();

    tokio::task::spawn_blocking(move || {
        let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;

        let mut sql = format!(
            "SELECT sequence, id, organization, namespace, topic, event_key, payload, content_type, metadata, \
             timestamp, version, parent_id, correlation_id, causation_id, partition_id, partition_count \
             FROM {relation} WHERE sequence > ?1"
        );
        let mut params: Vec<Value> = vec![Value::Integer(after_seq)];
        let mut idx = 2usize;

        if let Some(stop) = stop_seq {
            sql.push_str(&format!(" AND sequence <= ?{idx}"));
            params.push(Value::Integer(stop));
            idx += 1;
        }

        if let Some(o) = &org {
            sql.push_str(&format!(" AND organization = ?{idx}"));
            params.push(Value::Text(o.clone()));
            idx += 1;
        }
        if let Some(t) = &exact_topic {
            sql.push_str(&format!(" AND topic = ?{idx}"));
            params.push(Value::Text(t.clone()));
            idx += 1;
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
        match &partitions {
            PartitionSelection::All => {}
            PartitionSelection::One(partition) => {
                sql.push_str(&format!(" AND partition_count = ?{idx}"));
                params.push(Value::Integer(partition.count() as i64));
                idx += 1;
                sql.push_str(&format!(" AND partition_id = ?{idx}"));
                params.push(Value::Integer(partition.id() as i64));
                idx += 1;
            }
            PartitionSelection::Many(group) => {
                sql.push_str(&format!(" AND partition_count = ?{idx}"));
                params.push(Value::Integer(group.count() as i64));
                idx += 1;
                let placeholders: Vec<String> = (0..group.partitions().len())
                    .map(|i| format!("?{}", idx + i))
                    .collect();
                sql.push_str(&format!(
                    " AND partition_id IN ({})",
                    placeholders.join(",")
                ));
                for partition in group.partitions() {
                    params.push(Value::Integer(partition.id() as i64));
                }
                idx += group.partitions().len();
            }
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
                let key: String = row.get(5)?;
                let payload_str: String = row.get(6)?;
                let content_type: String = row.get(7)?;
                let metadata_str: String = row.get(8)?;
                let timestamp_str: String = row.get(9)?;
                let version: i64 = row.get(10)?;
                let parent_id: Option<String> = row.get(11)?;
                let correlation_id: Option<String> = row.get(12)?;
                let causation_id: Option<String> = row.get(13)?;
                let partition_id: Option<i64> = row.get(14)?;
                let partition_count: Option<i64> = row.get(15)?;
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
                    partition_id,
                    partition_count,
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
                partition_id,
                partition_count,
            ) = row.map_err(|e| Error::Store(e.to_string()))?;

            let payload: SerializedPayload = serde_json::from_str(&payload_str)
                .map_err(|e| Error::Serialization(format!("decode payload: {e}")))?;
            let _ = content_type;
            let id = uuid::Uuid::parse_str(&id)
                .map_err(|e| Error::Serialization(format!("decode id: {e}")))?;
            let parent_id = parent_id
                .as_deref()
                .map(uuid::Uuid::parse_str)
                .transpose()
                .map_err(|e| Error::Serialization(format!("decode parent_id: {e}")))?;
            let metadata: HashMap<String, String> = serde_json::from_str(&metadata_str)
                .map_err(|e| Error::Serialization(format!("decode metadata: {e}")))?;
            let timestamp = DateTime::parse_from_rfc3339(&timestamp_str)
                .map(|d| d.with_timezone(&Utc))
                .map_err(|e| Error::Serialization(format!("decode timestamp: {e}")))?;
            let partition = match (partition_id, partition_count) {
                (Some(pid), Some(pcount)) => {
                    let id_u32 = u32::try_from(pid).map_err(|_| {
                        Error::Store(format!("partition_id {pid} exceeds u32::MAX"))
                    })?;
                    let count_u32 = u32::try_from(pcount).map_err(|_| {
                        Error::Store(format!("partition_count {pcount} exceeds u32::MAX"))
                    })?;
                    let count = NonZeroU32::new(count_u32).ok_or_else(|| {
                        Error::Store("partition_count must be positive".to_owned())
                    })?;
                    Partition::new(id_u32, count)
                        .map_err(|e| Error::Store(format!("invalid partition: {e}")))?
                }
                _ => {
                    return Err(Error::Store(
                        "event has NULL partition columns — run partition backfill first"
                            .to_owned(),
                    ));
                }
            };
            out.push((
                SerializedEvent {
                    id,
                    organization,
                    namespace,
                    topic,
                    payload,
                    metadata,
                    timestamp,
                    version: version as u64,
                    key,
                    parent_id,
                    correlation_id,
                    causation_id,
                },
                sequence,
                partition,
            ));
        }
        Ok(out)
    })
    .await
    .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
}

/// SQLite coordinated reader: thin alias over the generic core type.
///
/// Why these aliases live here:
///
/// `CoordinatedReader<R, Coord>` is fully generic in `eventuary-core` — it
/// composes any partition-aware `Reader<R>` with any `PartitionCoordinator<C>`
/// and contains no SQLite-specific code. Every backend therefore only needs
/// `pub type` aliases to specialize the generic with its own reader and
/// coordinator (`SqliteReader` + `SqlitePartitionCoordinator`). The same
/// module path shape is preserved across backends so users learn one structure
/// once.
///
/// These aliases shorten call sites — `SqliteCoordinatedReader` reads better
/// than `CoordinatedReader<SqliteReader, SqlitePartitionCoordinator>` — and
/// give the umbrella a canonical, discoverable path next to `writer` and
/// `coordinator` modules.
pub type SqlitePartitionedCursor = PartitionedCursor<SqliteCursor>;
pub type SqliteCoordinatedReaderConfig = CoordinatedReaderConfig;
pub type SqliteCoordinatedSubscription = CoordinatedSubscription<SqliteSubscription, SqliteCursor>;
pub type SqliteCoordinatedReader = CoordinatedReader<SqliteReader, SqlitePartitionCoordinator>;
/// Standalone `PartitionLease`-fenced acker over the raw `SqliteCursor`.
/// This alias matches the simple shape used by code paths that wire a
/// coordinator outside of `CoordinatedReader::read`. The stream-emitted
/// acker after the shared-fetch rewrite is [`SqliteCoordinatedStreamAcker`].
pub type SqliteCoordinatedAcker =
    CoordinatedAcker<SqliteCursorAcker, SqliteCursor, SqlitePartitionCoordinator>;
/// Acker carried on every message emitted by [`SqliteCoordinatedReader`].
pub type SqliteCoordinatedStreamAcker = CoordinatedAcker<
    PartitionAcker<SqliteCursorAcker, SqliteCursor>,
    PartitionedCursor<SqliteCursor>,
    PartitionedCoordAdapter<SqlitePartitionCoordinator, SqliteCursor>,
>;
pub type SqliteCoordinatedCursor = CoordinatedCursor<PartitionedCursor<SqliteCursor>>;
pub type SqliteCoordinatedStream = CoordinatedStream<
    PartitionAcker<SqliteCursorAcker, SqliteCursor>,
    PartitionedCursor<SqliteCursor>,
    PartitionedCoordAdapter<SqlitePartitionCoordinator, SqliteCursor>,
>;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use futures::StreamExt;
    use tokio::time::timeout;

    use super::*;
    use crate::database::SqliteDatabase;
    use crate::writer::{SqlitePartitioningConfig, SqliteWriter, SqliteWriterConfig};
    use eventuary_core::io::cursor::{CursorCodec, CursorOrder};
    use eventuary_core::io::{Cursor, CursorId, Reader, Writer};
    use eventuary_core::partition::{
        EventKeyPartitionKeyResolver, Fnv1a64PartitionHasher, PartitionHasher, PartitionKey,
    };
    use eventuary_core::{Event, PartitionableSubscription, Payload, StartFrom, StopAt};

    fn test_partition() -> Partition {
        Partition::new(0, NonZeroU32::new(1).unwrap()).unwrap()
    }

    #[test]
    fn sqlite_subscription_with_partition_wraps_in_singleton_partition_group() {
        let count = NonZeroU32::new(8).unwrap();
        let partition = Partition::new(3, count).unwrap();
        let sub = SqliteSubscription::default().with_partition(partition);
        match sub.partitions {
            PartitionSelection::Many(g) => {
                assert_eq!(g.len(), 1);
                assert_eq!(g.partitions()[0].id(), 3);
                assert_eq!(g.count(), 8);
            }
            _ => panic!("expected PartitionSelection::Many(singleton)"),
        }
    }

    #[test]
    fn sqlite_subscription_with_partitions_sets_partition_selection_many() {
        let count = NonZeroU32::new(8).unwrap();
        let group = PartitionGroup::new(vec![
            Partition::new(2, count).unwrap(),
            Partition::new(5, count).unwrap(),
        ])
        .unwrap();
        let sub = SqliteSubscription::default().with_partitions(group);
        match sub.partitions {
            PartitionSelection::Many(g) => {
                assert_eq!(g.len(), 2);
                let ids: Vec<u32> = g.partitions().iter().map(|p| p.id()).collect();
                assert_eq!(ids, vec![2, 5]);
            }
            _ => panic!("expected PartitionSelection::Many"),
        }
    }

    #[test]
    fn sqlite_cursor_id_is_global() {
        assert_eq!(
            SqliteCursor::new(42, test_partition()).id(),
            CursorId::global()
        );
    }

    #[test]
    fn sqlite_cursor_order_key_from_sequence() {
        assert_eq!(
            SqliteCursor::new(42, test_partition()).order_key(),
            CursorOrder::from_i64(42)
        );
        assert!(
            SqliteCursor::new(9, test_partition()).order_key()
                < SqliteCursor::new(10, test_partition()).order_key()
        );
    }

    #[test]
    fn sqlite_cursor_codec_roundtrips() {
        let codec = SqliteCursor::codec().unwrap();
        let cursor = SqliteCursor::new(42, test_partition());
        let encoded = codec.encode(&cursor).unwrap();
        assert_eq!(encoded.kind().as_str(), "eventuary.sqlite.sqlite_cursor.v1");
        assert_eq!(encoded.order(), &CursorOrder::from_i64(42));
        assert_eq!(codec.decode(&encoded).unwrap(), cursor);
    }

    #[test]
    fn sqlite_cursor_codec_preserves_typed_ord() {
        let codec = SqliteCursor::codec().unwrap();
        let lo = codec
            .encode(&SqliteCursor::new(9, test_partition()))
            .unwrap();
        let hi = codec
            .encode(&SqliteCursor::new(10, test_partition()))
            .unwrap();
        assert!(lo < hi);
    }

    const PARTITION_COUNT: u32 = 4;

    fn event_with_key(key: &str) -> Event {
        Event::builder(
            "acme",
            "/orders",
            "order.placed",
            key,
            Payload::from_string("{}"),
        )
        .unwrap()
        .build()
        .unwrap()
    }

    fn partition_for_key(key: &str) -> u32 {
        let k = PartitionKey::new(key).unwrap();
        let hash = Fnv1a64PartitionHasher.hash(&k);
        (hash.get() % PARTITION_COUNT as u64) as u32
    }

    fn fast_config() -> SqliteReaderConfig {
        SqliteReaderConfig {
            poll_interval: Duration::from_millis(10),
            ..SqliteReaderConfig::default()
        }
    }

    #[tokio::test]
    async fn reader_default_all_returns_every_event() {
        let db = SqliteDatabase::open_in_memory().unwrap();
        let config = SqliteWriterConfig {
            partitioning: SqlitePartitioningConfig::inline(
                NonZeroU32::new(PARTITION_COUNT).unwrap(),
                EventKeyPartitionKeyResolver::new(),
                Fnv1a64PartitionHasher,
            ),
            ..SqliteWriterConfig::default()
        };
        SqliteWriter::prepare_schema(&db.conn(), &config).unwrap();
        let writer = SqliteWriter::new_with_config(db.conn(), config);

        let keys = ["k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7"];
        for key in &keys {
            writer.write(&event_with_key(key)).await.unwrap();
        }

        let reader = SqliteReader::new(db.conn(), fast_config());
        let subscription = SqliteSubscription {
            start: StartFrom::Earliest,
            stop_at: StopAt::CurrentEnd,
            ..SqliteSubscription::default()
        };
        let mut stream = reader.read(subscription).await.unwrap();

        let mut count = 0usize;
        while let Ok(Some(Ok(msg))) = timeout(Duration::from_secs(5), stream.next()).await {
            msg.acker().ack().await.unwrap();
            count += 1;
        }

        assert_eq!(count, keys.len());
    }

    #[tokio::test]
    async fn reader_one_filters_to_single_partition() {
        let db = SqliteDatabase::open_in_memory().unwrap();
        let config = SqliteWriterConfig {
            partitioning: SqlitePartitioningConfig::inline(
                NonZeroU32::new(PARTITION_COUNT).unwrap(),
                EventKeyPartitionKeyResolver::new(),
                Fnv1a64PartitionHasher,
            ),
            ..SqliteWriterConfig::default()
        };
        SqliteWriter::prepare_schema(&db.conn(), &config).unwrap();
        let writer = SqliteWriter::new_with_config(db.conn(), config);

        let keys = ["k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7"];
        for key in &keys {
            writer.write(&event_with_key(key)).await.unwrap();
        }

        let partitions_by_id: HashMap<u32, Vec<&str>> =
            keys.iter().fold(HashMap::new(), |mut acc, key| {
                acc.entry(partition_for_key(key)).or_default().push(key);
                acc
            });

        let (chosen_partition, expected_keys) = partitions_by_id
            .iter()
            .find(|(_, ks)| ks.len() >= 2)
            .map(|(id, ks)| (*id, ks.clone()))
            .expect("expected at least one partition with >=2 events");

        let reader = SqliteReader::new(db.conn(), fast_config());
        let subscription = SqliteSubscription {
            start: StartFrom::Earliest,
            stop_at: StopAt::CurrentEnd,
            partitions: PartitionSelection::One(
                Partition::new(chosen_partition, NonZeroU32::new(PARTITION_COUNT).unwrap())
                    .unwrap(),
            ),
            ..SqliteSubscription::default()
        };
        let mut stream = reader.read(subscription).await.unwrap();

        let mut received_keys: Vec<String> = Vec::new();
        while let Ok(Some(Ok(msg))) = timeout(Duration::from_secs(5), stream.next()).await {
            let key = msg.event().key().as_str().to_owned();
            msg.acker().ack().await.unwrap();
            received_keys.push(key);
        }

        assert_eq!(received_keys.len(), expected_keys.len());
        for key in &received_keys {
            assert_eq!(
                partition_for_key(key),
                chosen_partition,
                "event key {key} maps to wrong partition"
            );
        }
    }

    #[tokio::test]
    async fn reader_many_filters_to_selected_partitions() {
        let db = SqliteDatabase::open_in_memory().unwrap();
        let config = SqliteWriterConfig {
            partitioning: SqlitePartitioningConfig::inline(
                NonZeroU32::new(PARTITION_COUNT).unwrap(),
                EventKeyPartitionKeyResolver::new(),
                Fnv1a64PartitionHasher,
            ),
            ..SqliteWriterConfig::default()
        };
        SqliteWriter::prepare_schema(&db.conn(), &config).unwrap();
        let writer = SqliteWriter::new_with_config(db.conn(), config);

        let keys = ["k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7"];
        for key in &keys {
            writer.write(&event_with_key(key)).await.unwrap();
        }

        let count = NonZeroU32::new(PARTITION_COUNT).unwrap();
        let mut populated: Vec<u32> = keys
            .iter()
            .map(|k| partition_for_key(k))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        populated.truncate(2);
        assert!(
            populated.len() >= 2,
            "fixture must populate at least 2 distinct partitions"
        );

        let selected: std::collections::HashSet<u32> = populated.iter().copied().collect();
        let expected_len = keys
            .iter()
            .copied()
            .filter(|k| selected.contains(&partition_for_key(k)))
            .count();

        let group = PartitionGroup::new(
            populated
                .iter()
                .map(|id| Partition::new(*id, count).unwrap())
                .collect(),
        )
        .unwrap();

        let reader = SqliteReader::new(db.conn(), fast_config());
        let subscription = SqliteSubscription {
            start: StartFrom::Earliest,
            stop_at: StopAt::CurrentEnd,
            ..SqliteSubscription::default()
        }
        .with_partitions(group);
        let mut stream = reader.read(subscription).await.unwrap();

        let mut received: Vec<String> = Vec::new();
        while let Ok(Some(Ok(msg))) = timeout(Duration::from_secs(5), stream.next()).await {
            let key = msg.event().key().as_str().to_owned();
            msg.acker().ack().await.unwrap();
            received.push(key);
        }

        assert_eq!(received.len(), expected_len);
        for key in &received {
            assert!(
                selected.contains(&partition_for_key(key)),
                "event key {key} maps to partition outside selected group"
            );
        }
    }
}
