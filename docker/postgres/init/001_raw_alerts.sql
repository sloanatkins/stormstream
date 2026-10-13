-- one row per alert, holding its latest state
CREATE TABLE IF NOT EXISTS raw_alerts (
    alert_id     TEXT PRIMARY KEY,
    event_type   TEXT NOT NULL,
    severity     TEXT NOT NULL CHECK (severity IN ('Extreme', 'Severe', 'Moderate', 'Minor', 'Unknown')),
    region       TEXT NOT NULL,
    effective_at TIMESTAMPTZ NOT NULL,
    expires_at   TIMESTAMPTZ,
    status       TEXT NOT NULL CHECK (status IN ('active', 'updated', 'expired')),
    ingested_at  TIMESTAMPTZ NOT NULL
);
