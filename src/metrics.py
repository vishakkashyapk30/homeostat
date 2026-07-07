"""Per-cycle metrics computed from the raw batch and the transform result.

`schema_hash` is computed over the *normalized* field names (post-alias), so
once the agent absorbs a drift with an alias, the drifted batch hashes back to
the canonical schema and is recognized as healthy again -- i.e. schema
evolution done safely.
"""

import hashlib

from .transform import normalize_record


def schema_hash(batch: list[dict], config: dict) -> str:
    aliases = config.get("field_aliases", {})
    fields: set = set()
    for raw in batch:
        fields.update(normalize_record(raw, aliases).keys())
    payload = ",".join(sorted(fields))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:12]


def null_rate_per_field(batch: list[dict], config: dict) -> dict:
    required = config["required_fields"]
    aliases = config.get("field_aliases", {})
    n = len(batch) or 1
    counts = {f: 0 for f in required}
    for raw in batch:
        rec = normalize_record(raw, aliases)
        for field in required:
            if rec.get(field) is None:
                counts[field] += 1
    return {f: round(c / n, 4) for f, c in counts.items()}


def duplicate_rate(batch: list[dict], config: dict) -> float:
    dedup_key = config.get("dedup_key", config["primary_key"])
    aliases = config.get("field_aliases", {})
    seen: set = set()
    dupes = 0
    for raw in batch:
        key = normalize_record(raw, aliases).get(dedup_key)
        if key in seen:
            dupes += 1
        else:
            seen.add(key)
    return round(dupes / (len(batch) or 1), 4)


def reason_counts(quarantine: list[dict]) -> dict:
    counts: dict = {}
    for item in quarantine:
        counts[item["reason"]] = counts.get(item["reason"], 0) + 1
    return counts


def compute_metrics(batch: list[dict], result: dict, config: dict) -> dict:
    total = len(batch)
    quarantine_count = len(result["quarantine"])
    return {
        "total_records": total,
        "clean_count": len(result["clean"]),
        "quarantine_count": quarantine_count,
        "deduped_count": result["deduped_count"],
        "quarantine_rate": round(quarantine_count / (total or 1), 4),
        "duplicate_rate": duplicate_rate(batch, config),
        "null_rate_per_field": null_rate_per_field(batch, config),
        "schema_hash": schema_hash(batch, config),
        "reason_counts": reason_counts(result["quarantine"]),
    }
