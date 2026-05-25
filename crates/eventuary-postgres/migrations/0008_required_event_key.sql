UPDATE {events}
SET event_key = id::text
WHERE event_key IS NULL;

ALTER TABLE {events} ALTER COLUMN event_key SET NOT NULL;
