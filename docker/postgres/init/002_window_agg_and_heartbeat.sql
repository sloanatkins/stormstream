-- Window counts and pipeline heartbeat
-- ====================================
--
-- alert_window_agg: how many alert events landed in each time window,
-- per region and severity. Drives the "Alert activity over time" chart.
--
-- pipeline_heartbeat: proof the Spark job is alive. Drives the pipeline
-- health box at the top of the dashboard.
--
-- Both are written by streaming/stream_alerts.py.
-- This file runs once, when the database volume is first created.

-- ── Window counts ────────────────────────────────────────────────────
-- alert_count is the number of alerts that showed up or changed in the
-- window (not the number of distinct alerts)
CREATE TABLE IF NOT EXISTS alert_window_agg (
    window_type  TEXT NOT NULL CHECK (window_type IN ('tumbling_5m', 'sliding_15m')),
    window_start TIMESTAMPTZ NOT NULL,
    window_end   TIMESTAMPTZ NOT NULL,
    region       TEXT NOT NULL,
    severity     TEXT NOT NULL CHECK (severity IN ('Extreme', 'Severe', 'Moderate', 'Minor', 'Unknown')),
    alert_count  INTEGER NOT NULL CHECK (alert_count >= 0),
    computed_at  TIMESTAMPTZ NOT NULL,
    -- A late alert makes spark recompute a window, so writes are upserts on this key
    PRIMARY KEY (window_type, window_start, region, severity)
);

-- ── Heartbeat ────────────────────────────────────────────────────────
-- One row per micro-batch. When nothing came in for 30 seconds the job
-- writes an idle row instead (batch_id = -1), so a quiet pipeline still
-- looks alive and a dead one does not
CREATE TABLE IF NOT EXISTS pipeline_heartbeat (
    id               BIGSERIAL PRIMARY KEY,
    batch_id         BIGINT NOT NULL,
    events_processed INTEGER NOT NULL,
    consumer_lag     INTEGER,               -- not filled in by the job yet
    written_at       TIMESTAMPTZ NOT NULL
);
