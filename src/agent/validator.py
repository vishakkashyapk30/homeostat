"""Validate a candidate config before promoting it (canary-style safety gate).

Re-runs the transform with the NEW config against the SAME raw batch that
triggered the incident. Only if the quarantine rate returns under the healthy
threshold (and the schema normalizes back to a known-good shape) is the fix
considered good.
"""

from ..metrics import compute_metrics
from ..orchestrator import load_raw
from ..transform import transform_batch


def validate(cycle_id: int, new_config: dict, last_ok: dict | None) -> tuple[bool, dict]:
    batch = load_raw(cycle_id)
    result = transform_batch(batch, new_config)
    metrics = compute_metrics(batch, result, new_config)

    threshold = new_config.get("quarantine_threshold", 0.05)
    schema_ok = last_ok is None or metrics["schema_hash"] == last_ok["metrics"]["schema_hash"]
    passed = metrics["quarantine_rate"] <= threshold and schema_ok
    return passed, metrics
