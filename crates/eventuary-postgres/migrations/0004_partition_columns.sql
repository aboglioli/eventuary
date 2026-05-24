ALTER TABLE {events} ADD COLUMN IF NOT EXISTS partition_key TEXT;
ALTER TABLE {events} ADD COLUMN IF NOT EXISTS partition_hash BIGINT;
ALTER TABLE {events} ADD COLUMN IF NOT EXISTS partition_id INT;
ALTER TABLE {events} ADD COLUMN IF NOT EXISTS partition_count INT;
ALTER TABLE {events} ADD COLUMN IF NOT EXISTS partition_strategy TEXT;

CREATE INDEX IF NOT EXISTS idx_events_partition_count_id_sequence
ON {events} (partition_count, partition_id, sequence)
WHERE partition_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_events_org_ns_partition_count_id_sequence
ON {events} (organization, namespace, partition_count, partition_id, sequence)
WHERE partition_id IS NOT NULL;
