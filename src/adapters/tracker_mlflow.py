"""Optional Databricks-native tracker: log every cycle + incident to MLflow.

MLflow is a Databricks product, so this turns the demo into an experiment-
tracked, auditable system: each cycle becomes an MLflow run (params, metrics,
status tag) and each incident logs the postmortem + config diff as artifacts.

It always ALSO writes the manifest, so the agent keeps working unchanged.

Enable with: pip install mlflow   (then --tracker mlflow)
"""

import json
import os

from .. import manifest, paths
from .tracker import Tracker


class MLflowTracker(Tracker):
    def __init__(self, experiment: str = "homeostat"):
        try:
            import mlflow
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "MLflowTracker requires the 'mlflow' package. Install it with: pip install mlflow"
            ) from exc

        self._mlflow = mlflow
        mlflow.set_experiment(experiment)

    def log_run(self, entry: dict) -> None:
        manifest.append_run(entry)
        mlflow = self._mlflow
        run_name = f"cycle_{entry['cycle_id']:03d}" + ("_healed" if entry.get("post_heal") else "")
        with mlflow.start_run(run_name=run_name):
            mlflow.log_param("cycle_id", entry["cycle_id"])
            mlflow.log_param("config_version", entry.get("transform_config_version"))
            mlflow.log_param("post_heal", entry.get("post_heal", False))
            mlflow.set_tag("status", entry.get("status"))
            m = entry.get("metrics", {})
            for key in ("total_records", "clean_count", "quarantine_count", "deduped_count",
                        "quarantine_rate", "duplicate_rate"):
                if key in m:
                    mlflow.log_metric(key, m[key])
            for field, rate in m.get("null_rate_per_field", {}).items():
                mlflow.log_metric(f"null_rate.{field}", rate)

    def log_incident(self, incident: dict) -> None:
        with open(paths.INCIDENTS_INDEX_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(incident) + "\n")

        mlflow = self._mlflow
        with mlflow.start_run(run_name=f"incident_cycle_{incident.get('cycle_id'):03d}"):
            mlflow.set_tag("classification", incident.get("classification"))
            mlflow.set_tag("resolution", incident.get("resolution"))
            mlflow.log_param("config_version_before", incident.get("config_version_before"))
            mlflow.log_param("config_version_after", incident.get("config_version_after"))
            before = incident.get("metrics_before") or {}
            after = incident.get("metrics_after") or {}
            if "quarantine_rate" in before:
                mlflow.log_metric("quarantine_rate_before", before["quarantine_rate"])
            if "quarantine_rate" in after:
                mlflow.log_metric("quarantine_rate_after", after["quarantine_rate"])
            if incident.get("config_diff") is not None:
                mlflow.log_dict(incident["config_diff"], "config_diff.json")
            postmortem_path = incident.get("postmortem_path")
            if postmortem_path and os.path.exists(postmortem_path):
                mlflow.log_artifact(postmortem_path)
