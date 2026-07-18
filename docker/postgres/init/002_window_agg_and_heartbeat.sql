-- counts per window, written by the spark job
CREATE TABLE IF NOT EXISTS alert_window_agg (
    window_type  TEXT NOT NULL CHECK (window_type IN ('tumbling_5m', 'sliding_15m')),
    window_start TIMESTAMPTZ NOT NULL,
    window_end   TIMESTAMPTZ NOT NULL,
    region       TEXT NOT NULL,
    severity     TEXT NOT NULL CHECK (severity IN ('Extreme', 'Severe', 'Moderate', 'Minor', 'Unknown')),
    alert_count  INTEGER NOT NULL CHECK (alert_count >= 0),
    computed_at  TIMESTAMPTZ NOT NULL,
    -- a late alert makes spark recompute a window, so writes are upserts on this key
    PRIMARY KEY (window_type, window_start, region, severity)
);

-- one row per spark micro-batch, drives the pipeline health panel
CREATE TABLE IF NOT EXISTS pipeline_heartbeat (
    id               BIGSERIAL PRIMARY KEY,
    batch_id         BIGINT NOT NULL,
    events_processed INTEGER NOT NULL,
    consumer_lag     INTEGER,
    written_at       TIMESTAMPTZ NOT NULL
);
