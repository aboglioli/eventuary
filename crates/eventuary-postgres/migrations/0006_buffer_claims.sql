CREATE TABLE IF NOT EXISTS {buffer_claims} (
    id              BIGSERIAL    PRIMARY KEY,
    event           JSONB        NOT NULL,
    claimed_by      TEXT         NULL,
    claimed_until   TIMESTAMPTZ  NULL,
    attempts        INT          NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_buffer_claims_pending
ON {buffer_claims} (claimed_until, id)
WHERE claimed_by IS NULL OR claimed_until IS NULL;

CREATE INDEX IF NOT EXISTS idx_buffer_claims_visibility
ON {buffer_claims} (claimed_until)
WHERE claimed_by IS NOT NULL;
