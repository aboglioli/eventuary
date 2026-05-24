CREATE TABLE IF NOT EXISTS {consumers} (
    consumer_group_id TEXT NOT NULL,
    stream_id         TEXT NOT NULL,
    owner_id          TEXT NOT NULL,
    lease_until       TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (consumer_group_id, stream_id, owner_id)
);

CREATE INDEX IF NOT EXISTS idx_event_stream_consumers_group_stream_lease
ON {consumers} (consumer_group_id, stream_id, lease_until);

CREATE TABLE IF NOT EXISTS {partitions} (
    consumer_group_id   TEXT        NOT NULL,
    stream_id           TEXT        NOT NULL,
    partition_id        INT         NOT NULL,
    owner_id            TEXT        NULL,
    lease_until         TIMESTAMPTZ NULL,
    checkpoint_sequence BIGINT      NOT NULL DEFAULT 0,
    generation          BIGINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (consumer_group_id, stream_id, partition_id)
);

CREATE INDEX IF NOT EXISTS idx_event_stream_partitions_group_stream_owner
ON {partitions} (consumer_group_id, stream_id, owner_id);
