"""Default tracker: append runs to the JSONL manifest + an incidents index."""

import json

from .. import manifest, paths
from .tracker import Tracker


class LocalTracker(Tracker):
    def log_run(self, entry: dict) -> None:
        manifest.append_run(entry)

    def log_incident(self, incident: dict) -> None:
        paths.ensure_dirs()
        with open(paths.INCIDENTS_INDEX_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(incident) + "\n")
