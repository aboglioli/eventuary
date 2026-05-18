CREATE TABLE IF NOT EXISTS {multiplexer_completions} (
    event_id      UUID        NOT NULL,
    subscriber_id TEXT        NOT NULL,
    completed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, subscriber_id)
);

CREATE TABLE IF NOT EXISTS {dedupe_keys} (
    event_id     UUID        NOT NULL PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS {buffer_entries} (
    id       BIGSERIAL    PRIMARY KEY,
    event    JSONB        NOT NULL,
    cursor   JSONB        NOT NULL,
    pushed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_buffer_entries_pushed_at ON {buffer_entries} (pushed_at);
