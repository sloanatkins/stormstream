-- dead_letter_events
-- ==================
--
-- Messages the Spark job could not use, kept so nothing disappears
-- silently. Each row has the raw text and the reason it was rejected,
-- like "not valid JSON or no known fields" or "missing or invalid field".
--
-- Written by: streaming/stream_alerts.py (the raw_alerts query)
-- Read by: the dashboard (Dead letters table)
--
-- This file runs once, when the database volume is first created.

CREATE TABLE IF NOT EXISTS dead_letter_events (
    id              BIGSERIAL PRIMARY KEY,
    raw             TEXT,                   -- the original message text, can be empty
    reason          TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,       -- where the message came from, handy for debugging
    kafka_offset    BIGINT NOT NULL,
    failed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- The same message read twice should not create two rows
    UNIQUE (kafka_partition, kafka_offset)
);
