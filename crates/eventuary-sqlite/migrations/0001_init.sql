CREATE TABLE IF NOT EXISTS {events} (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    organization TEXT NOT NULL,
    namespace TEXT NOT NULL,
    topic TEXT NOT NULL,
    event_key TEXT,
    payload TEXT NOT NULL,
    content_type TEXT NOT NULL,
    metadata TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    version INTEGER NOT NULL,
    parent_id TEXT,
    correlation_id TEXT,
    causation_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_events_org_seq ON {events} (organization, sequence);
CREATE INDEX IF NOT EXISTS idx_events_org_ns_seq ON {events} (organization, namespace, sequence);
CREATE INDEX IF NOT EXISTS idx_events_org_topic_seq ON {events} (organization, topic, sequence);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON {events} (timestamp);

CREATE TABLE IF NOT EXISTS {offsets} (
    consumer_group_id TEXT    NOT NULL,
    checkpoint_name   TEXT    NOT NULL DEFAULT 'default',
    partition         INTEGER NOT NULL DEFAULT 0,
    partition_count   INTEGER NOT NULL DEFAULT 1,
    sequence          INTEGER NOT NULL,
    PRIMARY KEY (consumer_group_id, checkpoint_name, partition, partition_count)
);
