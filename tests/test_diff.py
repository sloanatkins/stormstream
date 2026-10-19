"""
Poll Diff Tests
===============

Tests for poller/diff.py. They check that each poll only reports what
actually changed since the last one.

Covered:
- first poll: everything is new
- unchanged alerts send nothing (a new ingested_at alone is not a change)
- a new severity or expiry time is an update
- an alert that leaves the feed is expired and then forgotten
- duplicate ids in one response, and a poll that mixes all of the above

Run with:
    python -m pytest tests/test_diff.py -v

Dependencies:
- pytest
"""

from poller.diff import diff, fingerprint


# ── Helpers ───────────────────────────────────────────────────────────
def make_event(alert_id: str = "a1", **changes) -> dict:
    """Build a normal alert. Each test changes only the field it cares about."""

    ev = {
        "alert_id": alert_id,
        "event_type": "Flood Watch",
        "severity": "Minor",
        "region": "FL",
        "effective_at": "2026-10-13T18:00:00+00:00",
        "expires_at": "2026-10-14T00:00:00+00:00",
        "status": "active",
        "ingested_at": "2026-10-13T18:01:00+00:00",
    }
    ev.update(changes)
    return ev


def seen_from(*events: dict) -> dict:
    """Build what the seen-store would hold after those alerts were sent."""

    return {e["alert_id"]: {"fingerprint": fingerprint(e), "event": e} for e in events}


# ── New and unchanged alerts ──────────────────────────────────────────
def test_first_poll_sends_everything_as_active():
    to_send, new_seen = diff({}, [make_event("a1"), make_event("a2")])
    assert len(to_send) == 2
    assert all(e["status"] == "active" for e in to_send)
    assert set(new_seen) == {"a1", "a2"}


def test_unchanged_alert_sends_nothing():
    ev = make_event()
    to_send, new_seen = diff(seen_from(ev), [ev])
    assert to_send == []
    assert "a1" in new_seen


def test_ingested_at_alone_is_not_a_change():
    # The poller stamps a new ingested_at every poll, that must not look like an update
    old = make_event(ingested_at="2026-10-13T18:01:00+00:00")
    new = make_event(ingested_at="2026-10-13T18:05:00+00:00")
    to_send, _ = diff(seen_from(old), [new])
    assert to_send == []


# ── Updates ───────────────────────────────────────────────────────────
def test_severity_change_is_an_update():
    old = make_event(severity="Minor")
    to_send, _ = diff(seen_from(old), [make_event(severity="Severe")])
    assert len(to_send) == 1
    assert to_send[0]["status"] == "updated"
    assert to_send[0]["severity"] == "Severe"


def test_new_expiry_time_is_an_update():
    old = make_event(expires_at="2026-10-14T00:00:00+00:00")
    to_send, _ = diff(seen_from(old), [make_event(expires_at="2026-10-14T06:00:00+00:00")])
    assert len(to_send) == 1
    assert to_send[0]["status"] == "updated"


# ── Expired alerts ────────────────────────────────────────────────────
def test_alert_that_leaves_the_feed_is_expired():
    ev = make_event()
    to_send, new_seen = diff(seen_from(ev), [])
    assert len(to_send) == 1
    assert to_send[0]["alert_id"] == "a1"
    assert to_send[0]["status"] == "expired"
    assert to_send[0]["ingested_at"] != ev["ingested_at"]  # stamped with the time we noticed
    assert new_seen == {}  # and we stop tracking it


# ── Edge cases ────────────────────────────────────────────────────────
def test_same_id_twice_in_one_response_keeps_the_last():
    first = make_event(severity="Minor")
    second = make_event(severity="Severe")
    to_send, _ = diff({}, [first, second])
    assert len(to_send) == 1
    assert to_send[0]["severity"] == "Severe"


def test_mixed_poll():
    stays = make_event("stays")
    changes = make_event("changes", severity="Minor")
    leaves = make_event("leaves")
    seen = seen_from(stays, changes, leaves)
    current = [stays, make_event("changes", severity="Severe"), make_event("brand-new")]
    to_send, _ = diff(seen, current)
    statuses = {e["alert_id"]: e["status"] for e in to_send}
    assert statuses == {"changes": "updated", "brand-new": "active", "leaves": "expired"}
