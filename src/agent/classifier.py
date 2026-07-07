"""Deterministic first-pass failure classifier.

Runs before any LLM call: fast, free, and reliable for the known failure
modes. The LLM is reserved for narrative and for the `unknown` catch-all.

Returns one of: schema_drift | null_spike | duplicate_keys | unknown | healthy.
"""

DOMINANCE = 0.5


def _dominant(reason_counts: dict, reason: str, total: int) -> bool:
    if total <= 0:
        return False
    return reason_counts.get(reason, 0) >= DOMINANCE * total


def classify(current: dict, last_ok: dict | None) -> str:
    metrics = current["metrics"]
    reason_counts = metrics.get("reason_counts", {})
    quarantine_count = metrics["quarantine_count"]
    schema_hash = metrics["schema_hash"]

    schema_changed = last_ok is not None and schema_hash != last_ok["metrics"]["schema_hash"]

    if quarantine_count == 0 and not schema_changed:
        return "healthy"

    if schema_changed or _dominant(reason_counts, "missing_field", quarantine_count):
        return "schema_drift"
    if _dominant(reason_counts, "null_violation", quarantine_count):
        return "null_spike"
    if _dominant(reason_counts, "duplicate_key", quarantine_count):
        return "duplicate_keys"
    return "unknown"
