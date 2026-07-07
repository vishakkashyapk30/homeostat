# Incident Postmortem — Cycle 004

**Detected At:** 2026-07-07T17:20:17.993100+00:00
**Failure Classification:** `schema_drift`
**Resolution Status:** `patch_applied`
**Config Version:** v1 -> v2
**Narrative Source:** gemini

## Diagnosis
All 300 incoming records are quarantined due to a missing field error for `item_sku`. This is caused by a schema drift where the source data provides `sku_code` instead of the expected `item_sku`.

**Root Cause:** The data pipeline is failing because the expected field `item_sku` is missing, as the incoming data uses `sku_code` instead.

## Evidence

### Metrics before
- total_records: 300
- clean_count: 0
- quarantine_count: 300
- quarantine_rate: 1.0
- deduped_count: 0
- duplicate_rate: 0.0
- schema_hash: `ba638cd0d87e`
- reason_counts: `{"missing_field": 300}`

### Metrics after
- total_records: 300
- clean_count: 300
- quarantine_count: 0
- quarantine_rate: 0.0
- deduped_count: 0
- duplicate_rate: 0.0
- schema_hash: `a0b680ad5d46`
- reason_counts: `{}`

### Sample quarantined record
```json
{
  "reason": "missing_field",
  "record": {
    "order_id": "ORD-004-00000",
    "user_id": "USR-00626",
    "quantity": 1,
    "price": 296.37,
    "timestamp": "2026-07-07T07:40:00Z",
    "region": "sa-east",
    "sku_code": "SKU-0051"
  }
}
```

## Action Taken
**Proposed action:** `patch_config`

**Config diff applied:**
```json
{
  "add_field_alias": {
    "sku_code": "item_sku"
  }
}
```

## Outcome
Patch validated: quarantine_rate dropped from 1.0 to 0.0. Promoted config v2 and re-ran the cycle successfully.
