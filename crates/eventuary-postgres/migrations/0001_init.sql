CREATE TABLE IF NOT EXISTS events (
    sequence BIGSERIAL PRIMARY KEY,
    id UUID NOT NULL UNIQUE,
    organization TEXT NOT NULL,
    namespace TEXT NOT NULL,
    topic TEXT NOT NULL,
    event_key TEXT,
    payload JSONB NOT NULL,
    content_type TEXT NOT NULL,
    metadata JSONB NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL,
    parent_id UUID,
    correlation_id TEXT,
    causation_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_events_org_sequence ON events (organization, sequence);
CREATE INDEX IF NOT EXISTS idx_events_org_topic_sequence ON events (organization, topic, sequence);
CREATE INDEX IF NOT EXISTS idx_events_org_namespace_sequence ON events (organization, namespace, sequence);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events (timestamp);

CREATE TABLE IF NOT EXISTS consumer_offsets (
    consumer_group_id TEXT NOT NULL,
    checkpoint_name TEXT NOT NULL DEFAULT 'default',
    sequence BIGINT NOT NULL,
    PRIMARY KEY (consumer_group_id, checkpoint_name)
);

CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE events ADD COLUMN IF NOT EXISTS parent_id UUID;
ALTER TABLE events ADD COLUMN IF NOT EXISTS correlation_id TEXT;
ALTER TABLE events ADD COLUMN IF NOT EXISTS causation_id TEXT;
ALTER TABLE events ALTER COLUMN event_key DROP NOT NULL;
