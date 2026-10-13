"""Compares this poll to the last one and says what actually changed."""
import hashlib
import json
from datetime import datetime, timezone


def fingerprint(event):
    # only the fields where a change actually matters
    key = json.dumps([event["event_type"], event["severity"], event["expires_at"]])
    return hashlib.sha1(key.encode()).hexdigest()


def diff(seen, current):
    """
    seen:    {alert_id: {"fingerprint": str, "event": dict}} from the last poll
    current: list of cleaned events from this poll
    returns: (events_to_send, new_seen)
    """
    # if the same id shows up twice in one response, keep the last one
    by_id = {ev["alert_id"]: ev for ev in current}

    to_send = []
    new_seen = {}
    for alert_id, ev in by_id.items():
        fp = fingerprint(ev)
        old = seen.get(alert_id)
        if old is None:
            to_send.append({**ev, "status": "active"})
        elif old["fingerprint"] != fp:
            to_send.append({**ev, "status": "updated"})
        new_seen[alert_id] = {"fingerprint": fp, "event": ev}

    # known last time but gone now -> expired
    for alert_id, old in seen.items():
        if alert_id not in new_seen:
            to_send.append({
                **old["event"],
                "status": "expired",
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            })

    return to_send, new_seen
