"""
Poll Diffing
============

Compares the current poll to the last one and works out what actually
changed, so only real changes get sent to Kafka instead of the whole
list every 45 seconds.

For each alert there are three outcomes:
- new (not seen before)        -> status "active"
- changed (fingerprint differs) -> status "updated"
- gone (seen before, now missing) -> status "expired"

Dependencies:
- standard library only
"""

import hashlib
import json
from datetime import datetime, timezone


def fingerprint(event: dict) -> str:
    """
    Hash the fields where a change actually matters.

    Only event_type, severity and expires_at count. Other fields
    changing (like the description) is not worth a new message.
    """

    key = json.dumps([event["event_type"], event["severity"], event["expires_at"]])
    return hashlib.sha1(key.encode()).hexdigest()


def diff(seen: dict, current: list[dict]) -> tuple[list[dict], dict]:
    """
    Work out what changed since the last poll.

    seen is what we remembered last time:
    {alert_id: {"fingerprint": str, "event": dict}}
    current is the list of cleaned events from this poll.

    Returns:
    --------
    tuple of:
    - events to send (new, updated and expired ones)
    - the new seen dict to save for the next poll
    """

    # If the same id shows up twice in one response keep the last one
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

    # Known last time but missing now means it expired.
    # It is not copied into new_seen, so it is forgotten after this
    for alert_id, old in seen.items():
        if alert_id not in new_seen:
            to_send.append({
                **old["event"],
                "status": "expired",
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            })

    return to_send, new_seen
