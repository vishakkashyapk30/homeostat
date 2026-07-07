from src.transform import transform_batch

BASE_CONFIG = {
    "version": 1,
    "primary_key": "order_id",
    "dedup_key": "order_id",
    "required_fields": ["order_id", "user_id", "item_sku", "price"],
    "field_types": {"order_id": "str", "user_id": "str", "item_sku": "str", "price": "float"},
    "field_aliases": {},
    "nullable_fields": [],
    "dedup_policy": "quarantine",
    "quarantine_threshold": 0.05,
}


def _rec(oid="1", user="u", sku="s", price=1.0):
    return {"order_id": oid, "user_id": user, "item_sku": sku, "price": price}


def test_clean_records_pass():
    result = transform_batch([_rec("1"), _rec("2")], BASE_CONFIG)
    assert len(result["clean"]) == 2
    assert result["quarantine"] == []


def test_missing_field_quarantined():
    bad = {"order_id": "1", "user_id": "u", "price": 1.0}  # no item_sku
    result = transform_batch([bad], BASE_CONFIG)
    assert result["clean"] == []
    assert result["quarantine"][0]["reason"] == "missing_field"


def test_null_violation_quarantined():
    bad = _rec("1")
    bad["user_id"] = None
    result = transform_batch([bad], BASE_CONFIG)
    assert result["quarantine"][0]["reason"] == "null_violation"


def test_nullable_field_allows_null():
    config = {**BASE_CONFIG, "nullable_fields": ["user_id"]}
    rec = _rec("1")
    rec["user_id"] = None
    result = transform_batch([rec], config)
    assert len(result["clean"]) == 1


def test_type_mismatch_quarantined():
    bad = _rec("1")
    bad["price"] = "not-a-number"
    result = transform_batch([bad], BASE_CONFIG)
    assert result["quarantine"][0]["reason"] == "type_mismatch"


def test_duplicate_quarantined_by_default():
    result = transform_batch([_rec("1"), _rec("1")], BASE_CONFIG)
    assert len(result["clean"]) == 1
    assert result["quarantine"][0]["reason"] == "duplicate_key"


def test_duplicate_dropped_when_policy_drop():
    config = {**BASE_CONFIG, "dedup_policy": "drop"}
    result = transform_batch([_rec("1"), _rec("1")], config)
    assert len(result["clean"]) == 1
    assert result["quarantine"] == []
    assert result["deduped_count"] == 1


def test_alias_absorbs_schema_drift():
    config = {**BASE_CONFIG, "field_aliases": {"sku_code": "item_sku"}}
    drifted = {"order_id": "1", "user_id": "u", "sku_code": "s", "price": 1.0}
    result = transform_batch([drifted], config)
    assert len(result["clean"]) == 1
    assert result["clean"][0]["item_sku"] == "s"
