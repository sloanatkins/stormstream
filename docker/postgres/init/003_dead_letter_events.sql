-- messages the spark job could not use, kept so nothing disappears silently
CREATE TABLE IF NOT EXISTS dead_letter_events (
    id              BIGSERIAL PRIMARY KEY,
    raw             TEXT,
    reason          TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset    BIGINT NOT NULL,
    failed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- same message read twice should not create two rows
    UNIQUE (kafka_partition, kafka_offset)
);
