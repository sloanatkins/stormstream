"""
Seen Store
==========

Small SQLite file so the poller remembers what it has seen after a
restart. Without it every restart would report all active alerts as
new again.

Dependencies:
- standard library only
"""

import json
import sqlite3


class SeenStore:
    """
    Saves and loads the poller's "seen" dict.

    The dict looks like {alert_id: {"fingerprint": str, "event": dict}},
    same as what diff() takes in and gives back.
    """

    def __init__(self, path: str):
        """Open the SQLite file at path, creating the table if needed."""

        self.conn = sqlite3.connect(path)
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS seen ("
            "alert_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, event TEXT NOT NULL)"
        )
        self.conn.commit()

    def load(self) -> dict:
        """
        Read everything that was saved last time.

        Returns:
        --------
        dict : alert_id -> {"fingerprint": str, "event": dict}
        """

        rows = self.conn.execute("SELECT alert_id, fingerprint, event FROM seen").fetchall()
        return {r[0]: {"fingerprint": r[1], "event": json.loads(r[2])} for r in rows}

    def save(self, seen: dict) -> None:
        """Replace the saved state with the new seen dict."""

        # One transaction: delete everything, write the new state.
        # If we crash halfway it rolls back, so there is no half-written state
        with self.conn:
            self.conn.execute("DELETE FROM seen")
            self.conn.executemany(
                "INSERT INTO seen (alert_id, fingerprint, event) VALUES (?, ?, ?)",
                [(k, v["fingerprint"], json.dumps(v["event"])) for k, v in seen.items()],
            )

    def close(self) -> None:
        """Close the database connection."""

        self.conn.close()
