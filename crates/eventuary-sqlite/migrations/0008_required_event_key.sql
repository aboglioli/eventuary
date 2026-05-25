UPDATE {events}
SET event_key = id
WHERE event_key IS NULL;

DROP TABLE IF EXISTS eventuary_required_key_migration_events;

CREATE TABLE eventuary_required_key_migration_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    organization TEXT NOT NULL,
    namespace TEXT NOT NULL,
    topic TEXT NOT NULL,
    event_key TEXT NOT NULL,
    payload TEXT NOT NULL,
    content_type TEXT NOT NULL,
    metadata TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    version INTEGER NOT NULL,
    parent_id TEXT,
    correlation_id TEXT,
    causation_id TEXT,
    partition_key TEXT,
    partition_hash INTEGER,
    partition_id INTEGER,
    partition_count INTEGER,
    partition_strategy TEXT
);

INSERT INTO eventuary_required_key_migration_events (
    sequence, id, organization, namespace, topic, event_key, payload, content_type,
    metadata, timestamp, version, parent_id, correlation_id, causation_id,
    partition_key, partition_hash, partition_id, partition_count, partition_strategy
)
SELECT
    sequence, id, organization, namespace, topic, event_key, payload, content_type,
    metadata, timestamp, version, parent_id, correlation_id, causation_id,
    partition_key, partition_hash, partition_id, partition_count, partition_strategy
FROM {events};

DROP TABLE {events};
ALTER TABLE eventuary_required_key_migration_events RENAME TO {events};

CREATE INDEX IF NOT EXISTS idx_events_org_seq ON {events} (organization, sequence);
CREATE INDEX IF NOT EXISTS idx_events_org_ns_seq ON {events} (organization, namespace, sequence);
CREATE INDEX IF NOT EXISTS idx_events_org_topic_seq ON {events} (organization, topic, sequence);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON {events} (timestamp);
CREATE INDEX IF NOT EXISTS idx_events_partition_count_id_sequence ON {events} (partition_count, partition_id, sequence) WHERE partition_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_org_ns_partition_count_id_sequence ON {events} (organization, namespace, partition_count, partition_id, sequence) WHERE partition_id IS NOT NULL;
