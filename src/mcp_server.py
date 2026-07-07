"""Homeostat MCP server.

Exposes the pipeline and the self-healing tools over the Model Context Protocol,
so ANY MCP client (Claude Desktop, Cursor, etc.) can drive Homeostat: run the
pipeline, inspect incidents, and heal a degraded cycle step by step using the
exact same guardrailed tools the in-process Gemini agent uses.

The healing tools reuse `AgentToolContext`, so the safety guarantees are
identical: `promote_candidate` refuses to promote an unvalidated fix.

Run with:  python -m src.mcp_server        (stdio transport)
"""

from mcp.server.fastmcp import FastMCP

from . import config_store, manifest, paths
from .adapters.sink_local import LocalSink
from .adapters.tracker_local import LocalTracker
from .agent.agentic import AgentToolContext
from .agent.loop import run_agent
from .failure_injector import FAILURE_TYPES, inject as inject_failure
from .ingest import get_next_batch
from .orchestrator import run_cycle

mcp = FastMCP("homeostat")

# A single in-progress healing session (candidate config + validation state).
_session: AgentToolContext | None = None


def _require_session() -> AgentToolContext | dict:
    if _session is None:
        return {"error": "No active incident. Call begin_incident(cycle_id) first."}
    return _session


def _parse_inject(spec: str) -> dict:
    inject_map: dict = {}
    for part in [p.strip() for p in spec.split(",") if p.strip()]:
        cycle_str, ftype = part.split(":", 1)
        if ftype not in FAILURE_TYPES:
            raise ValueError(f"Unknown failure type {ftype!r}; expected one of {FAILURE_TYPES}.")
        inject_map[int(cycle_str)] = ftype
    return inject_map


def _reset_state() -> None:
    import glob
    import os

    paths.ensure_dirs()
    for path in (paths.MANIFEST_PATH, paths.INCIDENTS_INDEX_PATH, paths.CLEAN_DB_PATH):
        if os.path.exists(path):
            os.remove(path)
    for directory, pattern in (
        (paths.RAW_DIR, "*.jsonl"),
        (paths.CLEAN_DIR, "*.jsonl"),
        (paths.QUARANTINE_DIR, "*.jsonl"),
        (paths.INCIDENTS_DIR, "*.md"),
    ):
        for f in glob.glob(os.path.join(directory, pattern)):
            os.remove(f)
    for f in glob.glob(os.path.join(paths.CONFIG_DIR, "transform_config_v*.json")):
        if os.path.basename(f) != "transform_config_v1.json":
            os.remove(f)
    config_store.set_active_version(1)


# --------------------------------------------------------------------------
# Pipeline control
# --------------------------------------------------------------------------
@mcp.tool()
def run_pipeline(cycles: int = 6, inject: str = "", auto_heal: bool = False, fresh: bool = True) -> dict:
    """Run N pipeline cycles.

    inject: comma-separated 'cycle:type' pairs, e.g. '4:schema_drift,7:null_spike'
            (types: schema_drift, null_spike, duplicate_keys, type_drift).
    auto_heal: if True, run the built-in rule-based agent on degraded cycles;
               if False, leave degraded cycles for you to heal via the tools.
    fresh: reset all state before running.
    """
    global _session
    _session = None
    inject_map = _parse_inject(inject)
    if fresh:
        _reset_state()

    sink = LocalSink()
    tracker = LocalTracker()
    results = []
    for cycle_id in range(1, cycles + 1):
        config = config_store.load_active_config()
        batch = get_next_batch(cycle_id)
        ftype = inject_map.get(cycle_id)
        if ftype:
            batch = inject_failure(batch, ftype, cycle_id)
        entry = run_cycle(cycle_id, batch, config, sink, tracker)
        row = {
            "cycle_id": cycle_id,
            "status": entry["status"],
            "config_version": entry["transform_config_version"],
            "quarantine_rate": entry["metrics"]["quarantine_rate"],
        }
        if entry["status"] == "degraded" and auto_heal:
            incident = run_agent(cycle_id, sink, tracker)
            row["healed"] = incident["resolution"]
        results.append(row)

    return {
        "cycles_run": cycles,
        "active_version": config_store.get_active_version(),
        "results": results,
        "degraded_cycles": [r["cycle_id"] for r in results if r["status"] == "degraded"],
    }


@mcp.tool()
def get_pipeline_status(limit: int = 8) -> dict:
    """Return recent run-manifest entries and which cycles are currently degraded."""
    runs = manifest.read_runs()
    recent = runs[-limit:]
    return {
        "active_version": config_store.get_active_version(),
        "degraded_cycles": sorted({e["cycle_id"] for e in runs if e.get("status") == "degraded"}),
        "recent_runs": [
            {
                "cycle_id": e["cycle_id"],
                "status": e["status"],
                "config_version": e["transform_config_version"],
                "post_heal": e.get("post_heal", False),
                "quarantine_rate": e["metrics"]["quarantine_rate"],
                "schema_hash": e["metrics"]["schema_hash"],
            }
            for e in recent
        ],
    }


# --------------------------------------------------------------------------
# Incident investigation + healing (reuses the agent's guardrailed tools)
# --------------------------------------------------------------------------
@mcp.tool()
def begin_incident(cycle_id: int) -> dict:
    """Open a healing session for a degraded cycle and return its incident report
    (metrics, reason codes, schema comparison, active config, quarantine sample)."""
    global _session
    _session = AgentToolContext(cycle_id, LocalSink(), LocalTracker())
    return _session.get_incident_report()


@mcp.tool()
def add_field_alias(source_field: str, target_field: str) -> dict:
    """Map an incoming (drifted) field name to the expected canonical name. Schema-drift fix."""
    s = _require_session()
    return s if isinstance(s, dict) else s.add_field_alias(source_field, target_field)


@mcp.tool()
def set_field_nullable(field: str) -> dict:
    """Relax the null policy for a required field. Null-spike fix."""
    s = _require_session()
    return s if isinstance(s, dict) else s.set_field_nullable(field)


@mcp.tool()
def set_dedup_policy(policy: str) -> dict:
    """Set duplicate handling: 'drop' or 'quarantine'. Duplicate-key fix uses 'drop'."""
    s = _require_session()
    return s if isinstance(s, dict) else s.set_dedup_policy(policy)


@mcp.tool()
def validate_candidate() -> dict:
    """Re-run the transform with the candidate config on the failing batch and report health."""
    s = _require_session()
    return s if isinstance(s, dict) else s.validate_candidate()


@mcp.tool()
def promote_candidate() -> dict:
    """Promote the candidate config and re-run the cycle. Refuses unless validation has passed."""
    s = _require_session()
    return s if isinstance(s, dict) else s.promote_candidate()


@mcp.tool()
def rollback() -> dict:
    """Revert the active config to the last known-good version (safety net)."""
    s = _require_session()
    return s if isinstance(s, dict) else s.rollback()


# --------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------
@mcp.tool()
def get_active_config() -> dict:
    """Return the currently active transform config."""
    return config_store.load_active_config()


@mcp.tool()
def list_incidents() -> list:
    """List generated incident postmortem filenames."""
    import os

    if not os.path.isdir(paths.INCIDENTS_DIR):
        return []
    return sorted(f for f in os.listdir(paths.INCIDENTS_DIR) if f.endswith(".md"))


@mcp.tool()
def read_postmortem(name: str) -> str:
    """Return the markdown content of a named incident postmortem."""
    import os

    safe = os.path.basename(name)
    path = os.path.join(paths.INCIDENTS_DIR, safe)
    if not os.path.exists(path):
        return f"Postmortem not found: {safe}"
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
