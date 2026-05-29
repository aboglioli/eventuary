use std::collections::{HashMap, VecDeque};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
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

use crate::coordinator::PgPartitionCoordinator;
use crate::event_log::{PgEventLogSchema, PgEventLogSchemaConfig};
use crate::relation::PgRelationName;

#[derive(
    Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct PgCursor {
    pub sequence: i64,
    pub partition: Partition,
}

impl PgCursor {
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

impl Cursor for PgCursor {
    fn order_key(&self) -> CursorOrder {
        CursorOrder::from_i64(self.sequence)
    }
}

impl HasPartition for PgCursor {
    fn partition(&self) -> Partition {
        self.partition
    }
}

impl PgCursor {
    pub fn codec() -> Result<JsonCursorCodec<Self>> {
        JsonCursorCodec::new("eventuary.postgres.pg_cursor.v1")
    }
}

#[derive(Debug, Clone)]
pub struct PgSubscription {
    pub start: StartFrom<PgCursor>,
    pub stop_at: StopAt<PgCursor>,
    pub filter: EventFilter,
    pub batch_size: Option<usize>,
    pub limit: Option<usize>,
    pub partitions: PartitionSelection,
}

impl Default for PgSubscription {
    fn default() -> Self {
        Self {
            start: StartFrom::Latest,
            stop_at: StopAt::Never,
            filter: EventFilter::default(),
            batch_size: None,
            limit: None,
            partitions: PartitionSelection::All,
        }
    }
}

impl StartableSubscription<PgCursor> for PgSubscription {
    fn with_start(mut self, start: StartFrom<PgCursor>) -> Self {
        self.start = start;
        self
    }
}

impl PartitionableSubscription<PgCursor> for PgSubscription {
    /// Restrict this subscription to a validated group of partitions sharing
    /// the same `partition_count`. The reader emits a single SQL query per
    /// poll using `partition_id = ANY($::bigint[])` instead of one query per
    /// partition. Single-partition uses fall through the default trait impl
    /// which wraps in a singleton group; `partition_id = ANY(ARRAY[$1])`
    /// plans identically to `partition_id = $1` on modern Postgres.
    fn with_partitions(mut self, group: PartitionGroup) -> Self {
        self.partitions = PartitionSelection::Many(group);
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
    notify: Arc<Notify>,
    sequence: i64,
}

struct CursorState {
    last_acked: i64,
    pending_nack: bool,
}

impl PgCursorAcker {
    #[doc(hidden)]
    pub fn dummy(sequence: i64) -> Self {
        Self {
            state: Arc::new(Mutex::new(CursorState {
                last_acked: 0,
                pending_nack: false,
            })),
            notify: Arc::new(Notify::new()),
            sequence,
        }
    }
}

impl Acker for PgCursorAcker {
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

#[derive(Clone)]
pub struct PgReader {
    pool: PgPool,
    config: PgReaderConfig,
}

impl PgReader {
    pub fn new(pool: PgPool, config: PgReaderConfig) -> Self {
        Self { pool, config }
    }

    pub async fn connect(pool: PgPool, config: PgReaderConfig) -> Result<Self> {
        Self::prepare_schema(&pool, &config).await?;
        Ok(Self::new(pool, config))
    }

    pub async fn prepare_schema(pool: &PgPool, config: &PgReaderConfig) -> Result<()> {
        PgEventLogSchema::prepare(
            pool,
            &PgEventLogSchemaConfig {
                events_relation: config.events_relation.clone(),
            },
        )
        .await
    }

    pub fn schema_sql(config: &PgReaderConfig) -> String {
        PgEventLogSchema::schema_sql(&PgEventLogSchemaConfig {
            events_relation: config.events_relation.clone(),
        })
    }
}

impl Reader for PgReader {
    type Subscription = PgSubscription;
    type Acker = PgCursorAcker;
    type Cursor = PgCursor;
    type Stream = SpawnedStream<PgCursorAcker, PgCursor>;

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
        let partitions = subscription.partitions.clone();

        let (mut after_seq, lower_bound_ts) =
            match resolve_initial_position(&pool, &events_relation, &subscription).await {
                Ok(pos) => pos,
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return Ok(SpawnedStream::from_receiver(rx));
                }
            };

        let stop_seq = match resolve_stop_position(&pool, &events_relation, &subscription).await {
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
                        &pool,
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
                    let acker = PgCursorAcker {
                        state: Arc::clone(&state),
                        notify: Arc::clone(&notify),
                        sequence,
                    };
                    let cursor = PgCursor {
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

pub type PgPartitionedCursor = PartitionedCursor<PgCursor>;
pub type PgCoordinatedReaderConfig = CoordinatedReaderConfig;
pub type PgCoordinatedSubscription = CoordinatedSubscription<PgSubscription, PgCursor>;
pub type PgCoordinatedReader = CoordinatedReader<PgReader, PgPartitionCoordinator>;
/// Standalone `PartitionLease`-fenced acker over the raw `PgCursor`. This
/// alias matches the simple shape used by code paths that wire a coordinator
/// outside of `CoordinatedReader::read`. The stream-emitted acker after the
/// shared-fetch rewrite is [`PgCoordinatedStreamAcker`].
pub type PgCoordinatedAcker = CoordinatedAcker<PgCursorAcker, PgCursor, PgPartitionCoordinator>;
/// Acker carried on every message emitted by [`PgCoordinatedReader`].
pub type PgCoordinatedStreamAcker = CoordinatedAcker<
    PartitionAcker<PgCursorAcker, PgCursor>,
    PartitionedCursor<PgCursor>,
    PartitionedCoordAdapter<PgPartitionCoordinator, PgCursor>,
>;
pub type PgCoordinatedCursor = CoordinatedCursor<PartitionedCursor<PgCursor>>;
pub type PgCoordinatedStream = CoordinatedStream<
    PartitionAcker<PgCursorAcker, PgCursor>,
    PartitionedCursor<PgCursor>,
    PartitionedCoordAdapter<PgPartitionCoordinator, PgCursor>,
>;

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

async fn resolve_stop_position(
    pool: &PgPool,
    events_relation: &str,
    subscription: &PgSubscription,
) -> Result<Option<i64>> {
    match subscription.stop_at {
        StopAt::Never => Ok(None),
        StopAt::Cursor(cursor) => Ok(Some(cursor.sequence)),
        StopAt::CurrentEnd => {
            let sql = match subscription.filter.organization.as_ref() {
                Some(_) => format!(
                    "SELECT COALESCE(MAX(sequence), 0) AS s FROM {events_relation} WHERE organization = $1",
                ),
                None => format!("SELECT COALESCE(MAX(sequence), 0) AS s FROM {events_relation}"),
            };
            let mut query = sqlx::query(&sql);
            if let Some(org) = subscription.filter.organization.as_ref() {
                query = query.bind(org.as_str());
            }
            let row = query
                .fetch_one(pool)
                .await
                .map_err(|e| Error::Store(e.to_string()))?;
            Ok(Some(row.get::<i64, _>("s")))
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
    pool: &PgPool,
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
    let mut sql = format!(
        "SELECT sequence, id::text AS id_text, organization, namespace, topic, event_key, \
         payload::text AS payload_text, content_type, metadata::text AS metadata_text, \
         timestamp::text AS timestamp_text, version, parent_id::text AS parent_id_text, \
         correlation_id, causation_id, partition_id, partition_count \
         FROM {events_relation} WHERE sequence > $1",
    );
    let mut bind_index = 2usize;

    if stop_seq.is_some() {
        sql.push_str(&format!(" AND sequence <= ${bind_index}"));
        bind_index += 1;
    }

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
        NamespacePattern::Prefix(ns) if !ns.is_root() => Some(ns.as_str().to_owned()),
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
    match partitions {
        PartitionSelection::All => {}
        PartitionSelection::One(_) => {
            sql.push_str(&format!(
                " AND partition_count = ${bind_index} AND partition_id = ${}",
                bind_index + 1
            ));
            bind_index += 2;
        }
        PartitionSelection::Many(_) => {
            sql.push_str(&format!(
                " AND partition_count = ${bind_index} AND partition_id = ANY(${}::bigint[])",
                bind_index + 1
            ));
            bind_index += 2;
        }
    }
    sql.push_str(&format!(" ORDER BY sequence ASC LIMIT ${bind_index}"));

    let mut q = sqlx::query(&sql).bind(after_seq);

    if let Some(stop_seq) = stop_seq {
        q = q.bind(stop_seq);
    }

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
    match partitions {
        PartitionSelection::All => {}
        PartitionSelection::One(partition) => {
            q = q.bind(partition.count() as i64);
            q = q.bind(partition.id() as i64);
        }
        PartitionSelection::Many(group) => {
            q = q.bind(group.count() as i64);
            let ids: Vec<i64> = group.partitions().iter().map(|p| p.id() as i64).collect();
            q = q.bind(ids);
        }
    }
    q = q.bind(take as i64);

    let rows = q
        .fetch_all(pool)
        .await
        .map_err(|e| Error::Store(e.to_string()))?;

    rows.into_iter()
        .map(|row| {
            let sequence: i64 = row.get("sequence");
            let id_text: String = row.get("id_text");
            let id = uuid::Uuid::parse_str(&id_text)
                .map_err(|e| Error::Serialization(format!("decode id: {e}")))?;
            let parent_id = row
                .get::<Option<String>, _>("parent_id_text")
                .as_deref()
                .map(uuid::Uuid::parse_str)
                .transpose()
                .map_err(|e| Error::Serialization(format!("decode parent_id: {e}")))?;
            let payload_str: String = row.get("payload_text");
            let payload: SerializedPayload = serde_json::from_str(&payload_str)
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
                payload,
                metadata,
                timestamp,
                version: row.get::<i64, _>("version") as u64,
                key: row.get("event_key"),
                parent_id,
                correlation_id: row.get("correlation_id"),
                causation_id: row.get("causation_id"),
            };
            let partition_id: Option<i64> = row.get("partition_id");
            let partition_count: Option<i64> = row.get("partition_count");
            let partition = match (partition_id, partition_count) {
                (Some(id), Some(count)) => {
                    let id_u32 = u32::try_from(id)
                        .map_err(|_| Error::Store(format!("partition_id {id} exceeds u32::MAX")))?;
                    let count_u32 = u32::try_from(count).map_err(|_| {
                        Error::Store(format!("partition_count {count} exceeds u32::MAX"))
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
            Ok((serialized, sequence, partition))
        })
        .collect()
}

fn parse_pg_timestamp(s: &str) -> std::result::Result<DateTime<Utc>, chrono::ParseError> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc));
    }
    DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%#z").map(|dt| dt.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventuary_core::io::cursor::{CursorCodec, CursorOrder};
    use eventuary_core::io::{Cursor, CursorId};

    fn test_partition() -> Partition {
        Partition::new(0, NonZeroU32::new(1).unwrap()).unwrap()
    }

    #[test]
    fn pg_subscription_with_partition_wraps_in_singleton_partition_group() {
        use eventuary_core::PartitionableSubscription;
        let count = NonZeroU32::new(8).unwrap();
        let partition = Partition::new(3, count).unwrap();
        let sub = PgSubscription::default().with_partition(partition);
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
    fn pg_subscription_with_partitions_sets_partition_selection_many() {
        let count = NonZeroU32::new(8).unwrap();
        let group = PartitionGroup::new(vec![
            Partition::new(1, count).unwrap(),
            Partition::new(4, count).unwrap(),
            Partition::new(7, count).unwrap(),
        ])
        .unwrap();
        let sub = PgSubscription::default().with_partitions(group);
        match sub.partitions {
            PartitionSelection::Many(g) => {
                assert_eq!(g.len(), 3);
                assert_eq!(g.count(), 8);
                let ids: Vec<u32> = g.partitions().iter().map(|p| p.id()).collect();
                assert_eq!(ids, vec![1, 4, 7]);
            }
            _ => panic!("expected PartitionSelection::Many"),
        }
    }

    #[test]
    fn pg_cursor_id_is_global() {
        assert_eq!(PgCursor::new(42, test_partition()).id(), CursorId::global());
    }

    #[test]
    fn pg_cursor_order_key_from_sequence() {
        assert_eq!(
            PgCursor::new(42, test_partition()).order_key(),
            CursorOrder::from_i64(42)
        );
        assert!(
            PgCursor::new(9, test_partition()).order_key()
                < PgCursor::new(10, test_partition()).order_key()
        );
    }

    #[test]
    fn pg_cursor_codec_roundtrips() {
        let codec = PgCursor::codec().unwrap();
        let cursor = PgCursor::new(42, test_partition());
        let encoded = codec.encode(&cursor).unwrap();
        assert_eq!(encoded.kind().as_str(), "eventuary.postgres.pg_cursor.v1");
        assert_eq!(encoded.order(), &CursorOrder::from_i64(42));
        assert_eq!(codec.decode(&encoded).unwrap(), cursor);
    }

    #[test]
    fn pg_cursor_codec_preserves_typed_ord() {
        let codec = PgCursor::codec().unwrap();
        let lo = codec.encode(&PgCursor::new(9, test_partition())).unwrap();
        let hi = codec.encode(&PgCursor::new(10, test_partition())).unwrap();
        assert!(lo < hi);
    }
}
