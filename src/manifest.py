"""The run manifest is the single source of truth the agent reads.

One JSON line per pipeline run. Backend adapters (local/MLflow) all funnel
through here so the agent's view of the world is backend-independent.
"""

import json

from . import paths


def append_run(entry: dict) -> None:
    paths.ensure_dirs()
    with open(paths.MANIFEST_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


def read_runs() -> list[dict]:
    try:
        with open(paths.MANIFEST_PATH, "r", encoding="utf-8") as f:
            return [json.loads(line) for line in f if line.strip()]
    except FileNotFoundError:
        return []


def last_ok_run(runs: list[dict] | None = None) -> dict | None:
    if runs is None:
        runs = read_runs()
    for entry in reversed(runs):
        if entry.get("status") == "ok":
            return entry
    return None
