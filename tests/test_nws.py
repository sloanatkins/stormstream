"""
NWS Parsing Tests
=================

Tests for poller/nws.py. They check that a raw NWS alert is cleaned
into the format the pipeline expects, and that alerts missing something
we can't work without are rejected.

Covered:
- a good alert is cleaned (id, event, severity, region, status)
- times are converted to UTC
- missing or unknown severity falls back to "Unknown"
- missing id, event, region or bad times raise BadAlert

Run with:
    python -m pytest tests/test_nws.py -v

Dependencies:
- pytest
"""

import pytest

from poller.nws import BadAlert, parse_feature, to_utc


# ── Helpers ───────────────────────────────────────────────────────────
def make_feature(**prop_changes) -> dict:
    """Build a fake NWS feature. Pass fields to override the defaults."""

    # Shaped like one item from the NWS "features" list
    props = {
        "id": "urn:oid:test.1",
        "event": "Flood Watch",
        "severity": "Severe",
        "effective": "2026-10-13T14:00:00-04:00",
        "expires": "2026-10-13T20:00:00-04:00",
        "geocode": {"UGC": ["FLC069", "FLC095"]},
    }
    props.update(prop_changes)
    return {"properties": props}


# ── A good alert ──────────────────────────────────────────────────────
def test_good_feature_is_cleaned():
    ev = parse_feature(make_feature())
    assert ev["alert_id"] == "urn:oid:test.1"
    assert ev["event_type"] == "Flood Watch"
    assert ev["severity"] == "Severe"
    assert ev["region"] == "FL"  # first two letters of the first UGC code
    assert ev["status"] == "active"


def test_times_are_converted_to_utc():
    ev = parse_feature(make_feature())
    assert ev["effective_at"] == "2026-10-13T18:00:00+00:00"  # 2pm eastern
    assert ev["expires_at"] == "2026-10-14T00:00:00+00:00"


def test_missing_expiry_becomes_none():
    ev = parse_feature(make_feature(expires=None))
    assert ev["expires_at"] is None


# ── Severity fallback ─────────────────────────────────────────────────
def test_unknown_severity_falls_back_to_unknown():
    ev = parse_feature(make_feature(severity="Catastrophic"))
    assert ev["severity"] == "Unknown"


def test_missing_severity_falls_back_to_unknown():
    ev = parse_feature(make_feature(severity=None))
    assert ev["severity"] == "Unknown"


# ── Rejected alerts ───────────────────────────────────────────────────
@pytest.mark.parametrize("field", ["id", "event"])
def test_missing_required_field_is_rejected(field: str):
    with pytest.raises(BadAlert):
        parse_feature(make_feature(**{field: None}))


def test_missing_ugc_code_is_rejected():
    with pytest.raises(BadAlert):
        parse_feature(make_feature(geocode={"UGC": []}))


def test_missing_geocode_is_rejected():
    with pytest.raises(BadAlert):
        parse_feature(make_feature(geocode=None))


def test_bad_effective_time_is_rejected():
    with pytest.raises(BadAlert):
        parse_feature(make_feature(effective="not a date"))


def test_feature_without_properties_is_rejected():
    with pytest.raises(BadAlert):
        parse_feature({})


# ── Time conversion ───────────────────────────────────────────────────
def test_to_utc_handles_other_offsets():
    assert to_utc("2026-10-13T10:11:00-05:00", "x") == "2026-10-13T15:11:00+00:00"
    assert to_utc("2026-10-13T15:11:00+00:00", "x") == "2026-10-13T15:11:00+00:00"
