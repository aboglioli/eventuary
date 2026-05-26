use std::fmt;
use std::num::NonZeroU32;
use std::sync::Arc;

use eventuary_core::io::Writer;
use eventuary_core::partition::{
    PartitionHash, PartitionHasher, PartitionKey, PartitionKeyResolver, PartitionStrategy,
};
use eventuary_core::{Error, Event, Result, SerializedEvent};

use crate::database::SqliteConn;
use crate::event_log::{SqliteEventLogSchema, SqliteEventLogSchemaConfig};
use crate::relation::SqliteRelationName;

#[derive(Clone, Default)]
pub enum SqlitePartitioningConfig {
    #[default]
    Off,
    Inline {
        partition_count: NonZeroU32,
        key_resolver: Arc<dyn PartitionKeyResolver>,
        hasher: Arc<dyn PartitionHasher>,
    },
}

impl SqlitePartitioningConfig {
    pub fn inline(
        count: NonZeroU32,
        resolver: impl PartitionKeyResolver + 'static,
        hasher: impl PartitionHasher + 'static,
    ) -> Self {
        Self::Inline {
            partition_count: count,
            key_resolver: Arc::new(resolver),
            hasher: Arc::new(hasher),
        }
    }
}

impl fmt::Debug for SqlitePartitioningConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Off => write!(f, "SqlitePartitioningConfig::Off"),
            Self::Inline {
                partition_count, ..
            } => f
                .debug_struct("SqlitePartitioningConfig::Inline")
                .field("partition_count", partition_count)
                .finish(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SqliteWriterConfig {
    pub events_relation: SqliteRelationName,
    pub partitioning: SqlitePartitioningConfig,
}

impl Default for SqliteWriterConfig {
    fn default() -> Self {
        Self {
            events_relation: SqliteRelationName::new("events").expect("default events relation"),
            partitioning: SqlitePartitioningConfig::Off,
        }
    }
}

pub struct SqliteWriter {
    conn: SqliteConn,
    insert_sql: Arc<String>,
    partitioning: SqlitePartitioningConfig,
}

impl SqliteWriter {
    pub fn connect(conn: SqliteConn, config: SqliteWriterConfig) -> Result<Self> {
        Self::prepare_schema(&conn, &config)?;
        Ok(Self::new_with_config(conn, config))
    }

    pub fn prepare_schema(conn: &SqliteConn, config: &SqliteWriterConfig) -> Result<()> {
        let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
        SqliteEventLogSchema::prepare(
            &guard,
            &SqliteEventLogSchemaConfig {
                events_relation: config.events_relation.clone(),
            },
        )
    }

    pub fn schema_sql(config: &SqliteWriterConfig) -> String {
        SqliteEventLogSchema::schema_sql(&SqliteEventLogSchemaConfig {
            events_relation: config.events_relation.clone(),
        })
    }

    pub fn new(conn: SqliteConn) -> Self {
        Self::new_with_config(conn, SqliteWriterConfig::default())
    }

    pub fn new_with_config(conn: SqliteConn, config: SqliteWriterConfig) -> Self {
        let insert_sql = format!(
            "INSERT INTO {events} \
             (id, organization, namespace, topic, event_key, payload, content_type, metadata, \
             timestamp, version, parent_id, correlation_id, causation_id, \
             partition_key, partition_hash, partition_id, partition_count, partition_strategy) \
             VALUES \
             (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)",
            events = config.events_relation.render(),
        );
        Self {
            conn,
            insert_sql: Arc::new(insert_sql),
            partitioning: config.partitioning,
        }
    }

    fn partition_data(&self, event: &Event) -> Result<PartitionData> {
        match &self.partitioning {
            SqlitePartitioningConfig::Off => Ok(PartitionData::default()),
            SqlitePartitioningConfig::Inline {
                partition_count,
                key_resolver,
                hasher,
            } => {
                let partition_key = key_resolver.partition_key(event)?;
                let partition_hash = hasher.hash(&partition_key);
                let partition = hasher.partition_for(&partition_key, *partition_count);
                let partition_strategy = PartitionStrategy::new(hasher.strategy())?;
                Ok(PartitionData {
                    partition_key: Some(partition_key),
                    partition_hash: Some(partition_hash),
                    partition_id: Some(partition.id() as i64),
                    partition_count: Some(partition.count() as i64),
                    partition_strategy: Some(partition_strategy),
                })
            }
        }
    }
}

impl Writer for SqliteWriter {
    async fn write(&self, event: &Event) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        let pd = self.partition_data(event)?;
        let event = event.clone();
        let sql = Arc::clone(&self.insert_sql);
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            insert_event(&guard, &sql, &event, &pd)
        })
        .await
        .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
    }

    async fn write_all(&self, events: &[Event]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let conn = Arc::clone(&self.conn);
        let partition_data: Result<Vec<PartitionData>> =
            events.iter().map(|e| self.partition_data(e)).collect();
        let partition_data = partition_data?;
        let events = events.to_vec();
        let sql = Arc::clone(&self.insert_sql);
        tokio::task::spawn_blocking(move || {
            let mut guard = conn.lock().map_err(|e| Error::Store(e.to_string()))?;
            let tx = guard
                .transaction()
                .map_err(|e| Error::Store(e.to_string()))?;
            for (event, pd) in events.iter().zip(partition_data.iter()) {
                insert_event(&tx, &sql, event, pd)?;
            }
            tx.commit().map_err(|e| Error::Store(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| Error::Store(format!("blocking task panicked: {e}")))?
    }
}

#[derive(Default)]
struct PartitionData {
    partition_key: Option<PartitionKey>,
    partition_hash: Option<PartitionHash>,
    partition_id: Option<i64>,
    partition_count: Option<i64>,
    partition_strategy: Option<PartitionStrategy>,
}

fn insert_event(
    conn: &rusqlite::Connection,
    sql: &str,
    event: &Event,
    pd: &PartitionData,
) -> Result<()> {
    let serialized = SerializedEvent::from_event(event)?;
    let content_type = serialized.payload.content_type().to_string();
    let payload = serde_json::to_string(&serialized.payload)
        .map_err(|e| Error::Store(format!("encode payload: {e}")))?;
    let metadata = serde_json::to_string(&serialized.metadata)
        .map_err(|e| Error::Store(format!("encode metadata: {e}")))?;
    conn.execute(
        sql,
        rusqlite::params![
            serialized.id.to_string(),
            serialized.organization,
            serialized.namespace,
            serialized.topic,
            serialized.key,
            payload,
            content_type,
            metadata,
            serialized.timestamp.to_rfc3339(),
            serialized.version as i64,
            serialized.parent_id.map(|id| id.to_string()),
            serialized.correlation_id,
            serialized.causation_id,
            pd.partition_key.as_ref().map(|k| k.as_str()),
            pd.partition_hash.map(|h| h.to_sql_i64()),
            pd.partition_id,
            pd.partition_count,
            pd.partition_strategy.as_ref().map(|s| s.as_str()),
        ],
    )
    .map_err(|e| Error::Store(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use eventuary_core::io::Writer;
    use eventuary_core::partition::{EventKeyPartitionKeyResolver, Fnv1a64PartitionHasher};
    use eventuary_core::{Event, Payload};

    use super::{SqlitePartitioningConfig, SqliteWriter, SqliteWriterConfig};
    use crate::database::SqliteDatabase;

    type PartitionRow = (
        Option<String>,
        Option<i64>,
        Option<i64>,
        Option<i64>,
        Option<String>,
    );

    fn keyed_event(key: &str) -> Event {
        Event::builder(
            "acme",
            "/orders",
            "order.created",
            key,
            Payload::from_string("{}"),
        )
        .unwrap()
        .build()
        .unwrap()
    }

    fn query_partition_row(conn: &rusqlite::Connection, event_id: &str) -> PartitionRow {
        conn.query_row(
            "SELECT partition_key, partition_hash, partition_id, partition_count, partition_strategy FROM events WHERE id = ?1",
            rusqlite::params![event_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn writer_off_partitioning_leaves_columns_null() {
        let db = SqliteDatabase::open_in_memory().unwrap();
        SqliteWriter::prepare_schema(&db.conn(), &SqliteWriterConfig::default()).unwrap();
        let writer = SqliteWriter::new(db.conn());
        let event = keyed_event("order-123");
        writer.write(&event).await.unwrap();

        let conn = db.conn();
        let guard = conn.lock().unwrap();
        let (pk, ph, pi, pc, ps) = query_partition_row(&guard, &event.id().as_uuid().to_string());

        assert!(pk.is_none());
        assert!(ph.is_none());
        assert!(pi.is_none());
        assert!(pc.is_none());
        assert!(ps.is_none());
    }

    #[tokio::test]
    async fn writer_inline_partitioning_persists_all_columns() {
        let db = SqliteDatabase::open_in_memory().unwrap();
        let config = SqliteWriterConfig {
            partitioning: SqlitePartitioningConfig::inline(
                NonZeroU32::new(64).unwrap(),
                EventKeyPartitionKeyResolver::new(),
                Fnv1a64PartitionHasher,
            ),
            ..SqliteWriterConfig::default()
        };
        SqliteWriter::prepare_schema(&db.conn(), &config).unwrap();
        let writer = SqliteWriter::new_with_config(db.conn(), config);
        let event = keyed_event("order-123");
        writer.write(&event).await.unwrap();

        let conn = db.conn();
        let guard = conn.lock().unwrap();
        let (pk, ph, pi, pc, ps) = query_partition_row(&guard, &event.id().as_uuid().to_string());

        assert_eq!(pk.as_deref(), Some("order-123"));
        assert_eq!(ph, Some(0x1b96f9c28b5d5aba_u64 as i64));
        assert_eq!(pi, Some((0x1b96f9c28b5d5aba_u64 % 64) as i64));
        assert_eq!(pc, Some(64));
        assert_eq!(ps.as_deref(), Some("fnv1a64:v1"));
    }
}
