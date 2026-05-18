CREATE TABLE IF NOT EXISTS {multiplexer_completions} (
    event_id      TEXT NOT NULL,
    subscriber_id TEXT NOT NULL,
    completed_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (event_id, subscriber_id)
);

CREATE TABLE IF NOT EXISTS {dedupe_keys} (
    event_id     TEXT NOT NULL PRIMARY KEY,
    processed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS {buffer_entries} (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    event     TEXT    NOT NULL,
    cursor    TEXT    NOT NULL,
    pushed_at TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_buffer_entries_pushed_at ON {buffer_entries} (pushed_at);

CREATE TABLE IF NOT EXISTS {watermarks} (
    key        TEXT NOT NULL PRIMARY KEY,
    ts         TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
