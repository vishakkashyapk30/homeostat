"""Default, zero-infra sink: SQLite for clean records + JSONL dead-letter files.

Writes are idempotent per cycle (existing rows/files for the cycle are replaced)
so the agent can safely re-run a cycle after applying a fix.
"""

import json
import sqlite3

from .. import paths
from .sink import Sink


class LocalSink(Sink):
    def __init__(self):
        paths.ensure_dirs()
        self.db_path = paths.CLEAN_DB_PATH
        self._init_db()

    def _init_db(self) -> None:
        con = sqlite3.connect(self.db_path)
        try:
            con.execute(
                "CREATE TABLE IF NOT EXISTS clean_orders "
                "(cycle_id INTEGER, order_id TEXT, payload TEXT)"
            )
            con.commit()
        finally:
            con.close()

    def write_clean(self, cycle_id: int, records: list[dict]) -> str:
        con = sqlite3.connect(self.db_path)
        try:
            con.execute("DELETE FROM clean_orders WHERE cycle_id = ?", (cycle_id,))
            con.executemany(
                "INSERT INTO clean_orders (cycle_id, order_id, payload) VALUES (?, ?, ?)",
                [(cycle_id, r.get("order_id"), json.dumps(r)) for r in records],
            )
            con.commit()
        finally:
            con.close()

        path = paths.clean_path(cycle_id)
        with open(path, "w", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r) + "\n")
        return path

    def write_quarantine(self, cycle_id: int, records_with_reasons: list[dict]) -> str:
        path = paths.quarantine_path(cycle_id)
        with open(path, "w", encoding="utf-8") as f:
            for item in records_with_reasons:
                f.write(json.dumps({"reason": item["reason"], "record": item["record"]}) + "\n")
        return path
