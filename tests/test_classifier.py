from src.agent.classifier import classify


def _entry(schema_hash="AAA", quarantine_count=0, reason_counts=None, quarantine_rate=0.0):
    return {
        "metrics": {
            "schema_hash": schema_hash,
            "quarantine_count": quarantine_count,
            "quarantine_rate": quarantine_rate,
            "reason_counts": reason_counts or {},
        }
    }


LAST_OK = _entry(schema_hash="AAA")


def test_healthy():
    assert classify(_entry(), LAST_OK) == "healthy"


def test_schema_drift_by_hash_change():
    current = _entry(schema_hash="BBB", quarantine_count=100, reason_counts={"missing_field": 100})
    assert classify(current, LAST_OK) == "schema_drift"


def test_schema_drift_by_missing_field_dominance():
    current = _entry(schema_hash="AAA", quarantine_count=100, reason_counts={"missing_field": 80})
    assert classify(current, LAST_OK) == "schema_drift"


def test_null_spike():
    current = _entry(schema_hash="AAA", quarantine_count=100, reason_counts={"null_violation": 90})
    assert classify(current, LAST_OK) == "null_spike"


def test_duplicate_keys():
    current = _entry(schema_hash="AAA", quarantine_count=100, reason_counts={"duplicate_key": 70})
    assert classify(current, LAST_OK) == "duplicate_keys"


def test_unknown():
    current = _entry(schema_hash="AAA", quarantine_count=100, reason_counts={"type_mismatch": 10}, quarantine_rate=0.1)
    assert classify(current, LAST_OK) == "unknown"
