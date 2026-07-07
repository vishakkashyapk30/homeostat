"""Optional Databricks-native sink: clean + dead-letter records to Delta Lake.

Delta Lake was created by Databricks, so this adapter is the highest-signal
backend for that target. The pipeline core never imports `deltalake` directly;
only this adapter does, and only when `--backend delta` is selected.

Enable with: pip install deltalake pyarrow
"""

import json
import os

from .. import paths
from .sink import Sink


class DeltaSink(Sink):
    def __init__(self, base_dir: str | None = None):
        try:
            import pyarrow  # noqa: F401
            from deltalake import write_deltalake  # noqa: F401
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "DeltaSink requires the 'deltalake' and 'pyarrow' packages. "
                "Install them with: pip install deltalake pyarrow"
            ) from exc

        self._write_deltalake = write_deltalake
        base = base_dir or os.path.join(paths.DATA_DIR, "delta")
        self.clean_table = os.path.join(base, "clean_orders")
        self.quarantine_table = os.path.join(base, "dead_letter")
        os.makedirs(base, exist_ok=True)

    def _write(self, table_path: str, rows: list[dict]) -> str:
        import pyarrow as pa

        if not rows:
            rows = [{}]
        # Normalize to JSON strings to keep an evolving schema simple and robust.
        table = pa.Table.from_pylist([{"payload": json.dumps(r)} for r in rows])
        self._write_deltalake(table_path, table, mode="append")
        return table_path

    def write_clean(self, cycle_id: int, records: list[dict]) -> str:
        tagged = [{**r, "_cycle_id": cycle_id} for r in records]
        return self._write(self.clean_table, tagged)

    def write_quarantine(self, cycle_id: int, records_with_reasons: list[dict]) -> str:
        tagged = [
            {"_cycle_id": cycle_id, "reason": item["reason"], "record": item["record"]}
            for item in records_with_reasons
        ]
        return self._write(self.quarantine_table, tagged)
