"""Small SQLite file so the poller remembers what it has seen after a restart."""
import json
import sqlite3


class SeenStore:
    def __init__(self, path):
        self.conn = sqlite3.connect(path)
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS seen ("
            "alert_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, event TEXT NOT NULL)"
        )
        self.conn.commit()

    def load(self):
        rows = self.conn.execute("SELECT alert_id, fingerprint, event FROM seen").fetchall()
        return {r[0]: {"fingerprint": r[1], "event": json.loads(r[2])} for r in rows}

    def save(self, seen):
        # one transaction: delete everything, write the new state.
        # if we crash halfway it rolls back, so no half-written state
        with self.conn:
            self.conn.execute("DELETE FROM seen")
            self.conn.executemany(
                "INSERT INTO seen (alert_id, fingerprint, event) VALUES (?, ?, ?)",
                [(k, v["fingerprint"], json.dumps(v["event"])) for k, v in seen.items()],
            )

    def close(self):
        self.conn.close()
