"""Talks to the NWS alerts API and cleans up what comes back."""
from datetime import datetime, timezone

import requests

NWS_URL = "https://api.weather.gov/alerts/active"
KNOWN_SEVERITIES = {"Extreme", "Severe", "Moderate", "Minor", "Unknown"}


class BadAlert(Exception):
    """An alert is missing something we can't work without."""


def fetch_active(area=None, timeout=15):
    # NWS wants a User-Agent so they can tell who is calling
    headers = {"User-Agent": "StormStream-portfolio-project", "Accept": "application/geo+json"}
    params = {"area": area} if area else None
    resp = requests.get(NWS_URL, headers=headers, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp.json()["features"]


def to_utc(value, field):
    # turns "2026-10-13T10:11:00-04:00" into the same moment in UTC
    try:
        dt = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        raise BadAlert(f"{field} is not a valid timestamp: {value!r}")
    return dt.astimezone(timezone.utc).isoformat()


def parse_feature(feature):
    props = feature.get("properties") or {}

    alert_id = props.get("id")
    if not alert_id:
        raise BadAlert("missing alert id")

    event_type = props.get("event")
    if not event_type:
        raise BadAlert("missing event type")

    # region is the kafka key, so no region means we can't send it.
    # UGC codes start with the state, like FLC069
    ugc = (props.get("geocode") or {}).get("UGC") or []
    if not ugc:
        raise BadAlert("missing UGC code, can't work out region")

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
