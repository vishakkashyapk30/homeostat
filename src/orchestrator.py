"""Runs a single pipeline cycle end-to-end and records the result.

A cycle: persist raw batch -> transform -> write clean + quarantine -> compute
metrics -> decide status -> log to the tracker (and thus the manifest).

Status is `degraded` when the quarantine rate exceeds the config threshold OR
the (normalized) schema hash has changed since the last healthy run. Note:
nothing about *what* was injected is ever written here -- the agent must infer
it from these metrics alone.
"""

import datetime
import json

from . import manifest, paths
from .adapters.sink import Sink
from .adapters.tracker import Tracker
from .metrics import compute_metrics
from .transform import transform_batch


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def write_raw(cycle_id: int, batch: list[dict]) -> str:
    paths.ensure_dirs()
    path = paths.raw_path(cycle_id)
    with open(path, "w", encoding="utf-8") as f:
        for record in batch:
            f.write(json.dumps(record) + "\n")
    return path


def load_raw(cycle_id: int) -> list[dict]:
    with open(paths.raw_path(cycle_id), "r", encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def decide_status(metrics: dict, last_ok: dict | None, config: dict) -> str:
    threshold = config.get("quarantine_threshold", 0.05)
    if metrics["quarantine_rate"] > threshold:
        return "degraded"
    if last_ok is not None and metrics["schema_hash"] != last_ok["metrics"]["schema_hash"]:
        return "degraded"
    return "ok"


def run_cycle(
    cycle_id: int,
    batch: list[dict],
    config: dict,
    sink: Sink,
    tracker: Tracker,
    post_heal: bool = False,
) -> dict:
    write_raw(cycle_id, batch)

    result = transform_batch(batch, config)
    sink.write_clean(cycle_id, result["clean"])
    sink.write_quarantine(cycle_id, result["quarantine"])

    metrics = compute_metrics(batch, result, config)
    last_ok = manifest.last_ok_run()
    status = decide_status(metrics, last_ok, config)

    entry = {
        "cycle_id": cycle_id,
        "timestamp": _now_iso(),
        "transform_config_version": config["version"],
        "status": status,
        "post_heal": post_heal,
        "metrics": metrics,
    }
    tracker.log_run(entry)
    return entry
