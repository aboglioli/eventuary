ALTER TABLE {events} ADD COLUMN partition_key TEXT;
ALTER TABLE {events} ADD COLUMN partition_hash INTEGER;
ALTER TABLE {events} ADD COLUMN partition_id INTEGER;
ALTER TABLE {events} ADD COLUMN partition_count INTEGER;
ALTER TABLE {events} ADD COLUMN partition_strategy TEXT;

CREATE INDEX IF NOT EXISTS idx_events_partition_count_id_sequence
ON {events} (partition_count, partition_id, sequence)
WHERE partition_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_events_org_ns_partition_count_id_sequence
ON {events} (organization, namespace, partition_count, partition_id, sequence)
WHERE partition_id IS NOT NULL;
