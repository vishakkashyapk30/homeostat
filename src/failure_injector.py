"""Controlled corruption of input batches to simulate real incidents.

IMPORTANT: the injector describes what it did only to the console operator.
It never writes that ground truth into the run manifest, because the agent must
diagnose failures from metrics/logs alone -- not from a cheat sheet.
"""

import random

FAILURE_TYPES = ("schema_drift", "null_spike", "duplicate_keys", "type_drift")


def inject(batch: list[dict], failure_type: str, cycle_id: int, base_seed: int = 42) -> list[dict]:
    rng = random.Random(base_seed * 7 + cycle_id)
    if failure_type == "schema_drift":
        return _schema_drift(batch)
    if failure_type == "null_spike":
        return _null_spike(batch, rng)
    if failure_type == "duplicate_keys":
        return _duplicate_keys(batch, rng)
    if failure_type == "type_drift":
        return _type_drift(batch)
    raise ValueError(f"Unknown failure_type: {failure_type!r}. Expected one of {FAILURE_TYPES}.")


def _schema_drift(batch: list[dict]) -> list[dict]:
    """Upstream renames `item_sku` -> `sku_code` (a classic schema evolution)."""
    out = []
    for record in batch:
        corrupted = dict(record)
        if "item_sku" in corrupted:
            corrupted["sku_code"] = corrupted.pop("item_sku")
        out.append(corrupted)
    return out


def _null_spike(batch: list[dict], rng: random.Random, field: str = "user_id", rate: float = 0.45) -> list[dict]:
    """A normally-populated field starts arriving null for ~`rate` of records."""
    out = []
    for record in batch:
        corrupted = dict(record)
        if rng.random() < rate:
            corrupted[field] = None
        out.append(corrupted)
    return out


def _duplicate_keys(batch: list[dict], rng: random.Random, rate: float = 0.15) -> list[dict]:
    """A fraction of records are re-delivered with the same primary key."""
    out = [dict(record) for record in batch]
    n_dupes = max(1, int(len(batch) * rate))
    for _ in range(n_dupes):
        out.append(dict(rng.choice(batch)))
    rng.shuffle(out)
    return out


def _type_drift(batch: list[dict]) -> list[dict]:
    """`price` starts arriving as a string. Intentionally NOT auto-fixable --
    used to demonstrate the rollback safety net for unrecognized failures."""
    out = []
    for record in batch:
        corrupted = dict(record)
        if "price" in corrupted and corrupted["price"] is not None:
            corrupted["price"] = str(corrupted["price"])
        out.append(corrupted)
    return out
