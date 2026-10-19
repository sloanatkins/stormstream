"""
NWS Alerts Client
=================

Talks to the National Weather Service alerts API and cleans up what
comes back into the flat alert format the rest of the pipeline uses.

Main pieces:
- fetch_active: calls the API and returns the raw alert features
- parse_feature: turns one raw feature into a clean alert dict
- BadAlert: raised when an alert is missing something we can't work without

Dependencies:
- requests
"""

from datetime import datetime, timezone

import requests

# ── Config ───────────────────────────────────────────────────────────
NWS_URL = "https://api.weather.gov/alerts/active"
KNOWN_SEVERITIES = {"Extreme", "Severe", "Moderate", "Minor", "Unknown"}


class BadAlert(Exception):
    """An alert is missing something we can't work without."""


# ── Fetching ─────────────────────────────────────────────────────────
def fetch_active(area: str | None = None, timeout: int = 15) -> list[dict]:
    """
    Fetch all currently active alerts from the NWS.

    area is an optional state code like "FL". Without it you get
    every active alert in the US.

    Returns:
    --------
    list of raw alert features (GeoJSON dicts)
    """

    # NWS wants a User-Agent so they can tell who is calling
    headers = {"User-Agent": "StormStream-portfolio-project", "Accept": "application/geo+json"}
    params = {"area": area} if area else None
    resp = requests.get(NWS_URL, headers=headers, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp.json()["features"]


# ── Cleaning ─────────────────────────────────────────────────────────
def to_utc(value: str | None, field: str) -> str:
    """
    Turn a timestamp with any offset into the same moment in UTC.

    "2026-10-13T10:11:00-04:00" becomes "2026-10-13T14:11:00+00:00".
    Raises BadAlert if the value is missing or not a real timestamp.
    """

    try:
        dt = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        raise BadAlert(f"{field} is not a valid timestamp: {value!r}")
    return dt.astimezone(timezone.utc).isoformat()


def parse_feature(feature: dict) -> dict:
    """
    Turn one raw NWS feature into a clean alert.

    Raises BadAlert if the id, event type, region or effective time
    is missing.

    Returns:
    --------
    dict with:
    - alert_id, event_type, severity, region
    - effective_at, expires_at (UTC, expires_at can be None)
    - status (always "active" here, the diff step decides the rest)
    - ingested_at (when we parsed it)
    """

    props = feature.get("properties") or {}

    alert_id = props.get("id")
    if not alert_id:
        raise BadAlert("missing alert id")

    event_type = props.get("event")
    if not event_type:
        raise BadAlert("missing event type")

    # Region is the kafka key, so no region means we can't send it.
    # UGC codes start with the state, like FLC069. Marine zones (PZ, AM)
    # and territories (GU, PR) end up as regions too
    ugc = (props.get("geocode") or {}).get("UGC") or []
    if not ugc:
        raise BadAlert("missing UGC code, can't work out region")

    # Anything we don't recognize is stored as Unknown
    severity = props.get("severity")
    if severity not in KNOWN_SEVERITIES:
        severity = "Unknown"

    expires = props.get("expires")

    return {
        "alert_id": alert_id,
        "event_type": event_type,
        "severity": severity,
        "region": ugc[0][:2],
        "effective_at": to_utc(props.get("effective"), "effective_at"),
        "expires_at": to_utc(expires, "expires_at") if expires else None,
        "status": "active",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }
