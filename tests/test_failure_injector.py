from src.failure_injector import inject
from src.ingest import get_next_batch


def test_schema_drift_renames_item_sku():
    batch = get_next_batch(1, size=50)
    drifted = inject(batch, "schema_drift", cycle_id=1)
    assert all("item_sku" not in r for r in drifted)
    assert all("sku_code" in r for r in drifted)


def test_null_spike_introduces_nulls():
    batch = get_next_batch(1, size=200)
    spiked = inject(batch, "null_spike", cycle_id=1)
    null_count = sum(1 for r in spiked if r["user_id"] is None)
    assert null_count > 0


def test_duplicate_keys_adds_records():
    batch = get_next_batch(1, size=100)
    dupes = inject(batch, "duplicate_keys", cycle_id=1)
    assert len(dupes) > len(batch)
    ids = [r["order_id"] for r in dupes]
    assert len(ids) != len(set(ids))


def test_type_drift_stringifies_price():
    batch = get_next_batch(1, size=30)
    drifted = inject(batch, "type_drift", cycle_id=1)
    assert all(isinstance(r["price"], str) for r in drifted)


def test_unknown_type_raises():
    try:
        inject([], "meltdown", cycle_id=1)
    except ValueError:
        return
    raise AssertionError("expected ValueError for unknown failure type")


def test_injection_is_deterministic():
    batch = get_next_batch(3, size=100)
    a = inject(batch, "null_spike", cycle_id=3)
    b = inject(batch, "null_spike", cycle_id=3)
    assert [r["user_id"] for r in a] == [r["user_id"] for r in b]
