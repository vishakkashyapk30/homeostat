"""Wires the self-healing agent pipeline for a single degraded cycle.

classify -> diagnose -> patch -> validate -> (promote + re-run | rollback)
-> postmortem. Returns an incident record describing what happened.
"""

import datetime
import json

from .. import config_store, manifest, paths
from ..adapters.sink import Sink
from ..adapters.tracker import Tracker
from ..orchestrator import load_raw, run_cycle
from . import patch_executor, rollback, validator
from .classifier import classify
from .diagnoser import diagnose
from .postmortem import write_postmortem


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _last_ok_before(runs: list[dict], current: dict) -> dict | None:
    idx = None
    for i, entry in enumerate(runs):
        if entry is current:
            idx = i
            break
    if idx is None:
        idx = len(runs) - 1
    for entry in reversed(runs[:idx]):
        if entry.get("status") == "ok":
            return entry
    return None


def _load_quarantine_sample(cycle_id: int, limit: int = 10) -> list[dict]:
    try:
        with open(paths.quarantine_path(cycle_id), "r", encoding="utf-8") as f:
            return [json.loads(line) for line in f if line.strip()][:limit]
    except FileNotFoundError:
        return []


def run_agent(cycle_id: int, sink: Sink, tracker: Tracker) -> dict:
    runs = manifest.read_runs()
    current = [e for e in runs if e["cycle_id"] == cycle_id][-1]
    last_ok = _last_ok_before(runs, current)

    active_config = config_store.load_active_config()
    classification = classify(current, last_ok)
    sample = _load_quarantine_sample(cycle_id)
    proposal = diagnose(classification, active_config, current, sample)

    incident = {
        "cycle_id": cycle_id,
        "detected_at": _now_iso(),
        "classification": classification,
        "proposal": proposal,
        "config_diff": proposal.get("config_diff"),
        "config_version_before": active_config["version"],
        "metrics_before": current["metrics"],
        "metrics_after": None,
        "config_version_after": active_config["version"],
        "resolution": None,
        "outcome_note": "",
    }

    action = proposal.get("proposed_action")

    if action == "patch_config" and proposal.get("config_diff"):
        new_config = patch_executor.apply_diff(active_config, proposal["config_diff"])
        patch_executor.write_new_version(new_config)
        passed, metrics_after = validator.validate(cycle_id, new_config, last_ok)
        incident["metrics_after"] = metrics_after

        if passed:
            config_store.set_active_version(new_config["version"])
            incident["config_version_after"] = new_config["version"]
            incident["resolution"] = "patch_applied"
            incident["outcome_note"] = (
                f"Patch validated: quarantine_rate dropped from "
                f"{current['metrics']['quarantine_rate']} to {metrics_after['quarantine_rate']}. "
                f"Promoted config v{new_config['version']} and re-ran the cycle successfully."
            )
            batch = load_raw(cycle_id)
            run_cycle(cycle_id, batch, new_config, sink, tracker, post_heal=True)
        else:
            reverted = rollback.rollback()
            incident["config_version_after"] = reverted
            incident["resolution"] = "patch_rejected"
            incident["outcome_note"] = (
                f"Candidate config v{new_config['version']} failed validation "
                f"(quarantine_rate {metrics_after['quarantine_rate']}). "
                f"Rejected and rolled back to v{reverted}."
            )
    else:
        reverted = rollback.rollback()
        incident["config_version_after"] = reverted
        incident["resolution"] = "rollback_executed"
        incident["outcome_note"] = (
            f"No safe patch available for classification '{classification}'. "
            f"Rolled back to last known-good config v{reverted}."
        )

    postmortem_path = write_postmortem(incident, sample)
    incident["postmortem_path"] = postmortem_path
    tracker.log_incident(incident)
    return incident
