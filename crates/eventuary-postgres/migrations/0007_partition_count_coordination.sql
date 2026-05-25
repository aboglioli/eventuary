ALTER TABLE {partitions}
ADD COLUMN IF NOT EXISTS partition_count INT NULL;

CREATE INDEX IF NOT EXISTS idx_event_stream_partitions_group_stream_count
ON {partitions} (consumer_group_id, stream_id, partition_count, partition_id);
