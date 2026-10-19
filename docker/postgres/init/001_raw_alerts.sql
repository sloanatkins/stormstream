-- raw_alerts
-- ==========
--
-- One row per alert, holding its latest state. The Spark job upserts into
-- this table, so an alert that changes just overwrites its own row.
--
-- Written by: streaming/stream_alerts.py (the raw_alerts query)
-- Read by: the dashboard (metrics, map and active alerts table)
--
-- This file runs once, when the database volume is first created.

CREATE TABLE IF NOT EXISTS raw_alerts (
    alert_id     TEXT PRIMARY KEY,          -- the NWS alert id
    event_type   TEXT NOT NULL,             -- like "Flood Watch"
    severity     TEXT NOT NULL CHECK (severity IN ('Extreme', 'Severe', 'Moderate', 'Minor', 'Unknown')),
    region       TEXT NOT NULL,             -- first two letters of the zone code, like "FL"
    effective_at TIMESTAMPTZ NOT NULL,
    expires_at   TIMESTAMPTZ,               -- can be empty, some alerts never expire
    status       TEXT NOT NULL CHECK (status IN ('active', 'updated', 'expired')),
    ingested_at  TIMESTAMPTZ NOT NULL       -- when the poller picked it up
);
