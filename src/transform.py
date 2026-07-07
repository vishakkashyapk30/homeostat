"""Transform layer: applies the active config's data-quality expectations.

A record is routed to `clean` only if it passes every expectation (schema,
nulls, types) and is not a duplicate. Anything else is either quarantined with
a machine-readable reason code or dropped as a deduplicated retry, depending on
the active dedup policy.
"""

_TYPE_CHECKS = {
    "str": lambda v: isinstance(v, str),
    "int": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "float": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "bool": lambda v: isinstance(v, bool),
}


def normalize_record(record: dict, aliases: dict) -> dict:
    """Apply field aliases so drifted field names map back to canonical ones."""
    rec = dict(record)
    for src, dst in aliases.items():
        if src in rec:
            if dst not in rec:
                rec[dst] = rec[src]
            del rec[src]
    return rec


def _type_ok(value, type_name: str) -> bool:
    check = _TYPE_CHECKS.get(type_name)
    if check is None:
        return True
    return check(value)


def _reason_for(rec: dict, config: dict):
    required = config["required_fields"]
    nullable = set(config.get("nullable_fields", []))
    field_types = config.get("field_types", {})

    missing = [f for f in required if f not in rec]
    if missing:
        return "missing_field"

    for field in required:
        if rec.get(field) is None and field not in nullable:
            return "null_violation"

    for field, type_name in field_types.items():
        value = rec.get(field)
        if value is not None and not _type_ok(value, type_name):
            return "type_mismatch"

    return None


def transform_batch(batch: list[dict], config: dict) -> dict:
    aliases = config.get("field_aliases", {})
    dedup_key = config.get("dedup_key", config["primary_key"])
    dedup_policy = config.get("dedup_policy", "quarantine")

    clean: list[dict] = []
    quarantine: list[dict] = []
    deduped_count = 0
    seen: set = set()

    for raw in batch:
        rec = normalize_record(raw, aliases)

        reason = _reason_for(rec, config)
        if reason is not None:
            quarantine.append({"reason": reason, "record": raw})
            continue

        key = rec.get(dedup_key)
        if key in seen:
            if dedup_policy == "drop":
                deduped_count += 1
            else:
                quarantine.append({"reason": "duplicate_key", "record": raw})
            continue

        seen.add(key)
        clean.append(rec)

    return {"clean": clean, "quarantine": quarantine, "deduped_count": deduped_count}
