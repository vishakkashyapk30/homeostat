"""A true tool-calling agent: Gemini reasons and drives the self-healing loop.

Unlike the deterministic `loop.run_agent`, here the LLM is the decision-maker.
It is given a set of TOOLS (investigate, patch, validate, promote, rollback,
finish) and must decide which to call, in what order, observing each result and
re-planning until the pipeline is healthy again.

Crucially, the tools keep deterministic guardrails: e.g. `promote_candidate`
refuses to run unless a validation has passed. So the model plans, but it cannot
corrupt state or promote an unproven fix -- the safety lives in the tools, not
in trusting the model.

If Gemini is unavailable, the caller falls back to the deterministic agent.
"""

import copy
import datetime
import json
import os

from .. import config_store, manifest, paths
from ..adapters.sink import Sink
from ..adapters.tracker import Tracker
from ..orchestrator import load_raw, run_cycle
from . import llm, rollback as rollback_mod
from .postmortem import write_postmortem
from .validator import validate as validate_config

MODEL_DEFAULT = "gemini-2.5-flash"
MAX_TOOL_CALLS = 20

SYSTEM_INSTRUCTION = """You are Homeostat, an autonomous SRE agent for a data pipeline.
A pipeline cycle has been flagged DEGRADED (too many records quarantined, or the
schema changed). Your goal is to restore it to a healthy state using the tools.

Follow this process:
1. Call get_incident_report first to gather evidence (metrics, reason codes,
   a sample of quarantined records, and the active config).
2. Reason about the failure type from the evidence:
   - A required field missing / a new unexpected field present -> a field was
     RENAMED upstream (schema drift). Fix with add_field_alias(source, target)
     where source is the new incoming name and target is the expected name.
   - A high null rate on a required field -> a null spike. Fix with
     set_field_nullable(field).
   - Many duplicate_key reasons -> duplicate re-deliveries. Fix with
     set_dedup_policy("drop").
   - An unrecognized signature (e.g. type mismatches you cannot safely map) ->
     do NOT guess. Call rollback.
3. After applying a fix, ALWAYS call validate_candidate before promoting.
4. If validation passed, call promote_candidate. If it failed, either try a
   different fix or call rollback.
5. Always end by calling finish with your classification, resolution, root_cause
   and a concise summary.

Rules: prefer the minimal change; never promote an unvalidated fix; when in
doubt, roll back rather than risk bad data."""


class AgentToolContext:
    """Holds mutable state and implements every tool the agent may call."""

    def __init__(self, cycle_id: int, sink: Sink, tracker: Tracker):
        self.cycle_id = cycle_id
        self.sink = sink
        self.tracker = tracker

        runs = manifest.read_runs()
        self.current = [e for e in runs if e["cycle_id"] == cycle_id][-1]
        self.last_ok = _last_ok_before(runs, self.current)

        self.active_config = config_store.load_active_config()
        self.candidate = copy.deepcopy(self.active_config)
        self.candidate["version"] = self.active_config["version"] + 1

        self.last_validation = None  # (passed: bool, metrics_after: dict)
        self.promoted = False
        self.rolled_back = False
        self.reverted_version = self.active_config["version"]
        self.finish_info = None
        # Capture the failing evidence now, before any post-heal re-run overwrites it.
        self.initial_sample = _quarantine_sample(cycle_id, 8)

    # ---- investigation -------------------------------------------------
    def get_incident_report(self) -> dict:
        m = self.current["metrics"]
        last_hash = self.last_ok["metrics"]["schema_hash"] if self.last_ok else None
        cfg = self.active_config
        return {
            "cycle_id": self.cycle_id,
            "status": self.current["status"],
            "quarantine_threshold": cfg.get("quarantine_threshold", 0.05),
            "metrics": {
                "total_records": m["total_records"],
                "clean_count": m["clean_count"],
                "quarantine_count": m["quarantine_count"],
                "quarantine_rate": m["quarantine_rate"],
                "duplicate_rate": m["duplicate_rate"],
                "null_rate_per_field": m["null_rate_per_field"],
                "reason_counts": m["reason_counts"],
            },
            "current_schema_hash": m["schema_hash"],
            "last_healthy_schema_hash": last_hash,
            "schema_changed": last_hash is not None and m["schema_hash"] != last_hash,
            "active_config": {
                "required_fields": cfg.get("required_fields"),
                "field_types": cfg.get("field_types"),
                "field_aliases": cfg.get("field_aliases", {}),
                "nullable_fields": cfg.get("nullable_fields", []),
                "dedup_policy": cfg.get("dedup_policy"),
            },
            "quarantine_sample": self.initial_sample,
        }

    # ---- candidate mutations (each invalidates prior validation) --------
    def add_field_alias(self, source_field: str, target_field: str) -> dict:
        self.candidate.setdefault("field_aliases", {})[source_field] = target_field
        self.last_validation = None
        return {"ok": True, "field_aliases": self.candidate["field_aliases"]}

    def set_field_nullable(self, field: str) -> dict:
        nullable = self.candidate.setdefault("nullable_fields", [])
        if field not in nullable:
            nullable.append(field)
        self.last_validation = None
        return {"ok": True, "nullable_fields": nullable}

    def set_dedup_policy(self, policy: str) -> dict:
        if policy not in ("quarantine", "drop"):
            return {"ok": False, "error": "policy must be 'quarantine' or 'drop'"}
        self.candidate["dedup_policy"] = policy
        self.last_validation = None
        return {"ok": True, "dedup_policy": policy}

    # ---- validate / promote / rollback (guardrails live here) ----------
    def validate_candidate(self) -> dict:
        passed, metrics_after = validate_config(self.cycle_id, self.candidate, self.last_ok)
        self.last_validation = (passed, metrics_after)
        return {
            "passed": passed,
            "quarantine_rate_after": metrics_after["quarantine_rate"],
            "schema_hash_after": metrics_after["schema_hash"],
        }

    def promote_candidate(self) -> dict:
        if not (self.last_validation and self.last_validation[0]):
            return {
                "ok": False,
                "error": "Refusing to promote: call validate_candidate and get passed=true first.",
            }
        config_store.save_config(self.candidate)
        config_store.set_active_version(self.candidate["version"])
        batch = load_raw(self.cycle_id)
        run_cycle(self.cycle_id, batch, self.candidate, self.sink, self.tracker, post_heal=True)
        self.promoted = True
        return {"ok": True, "active_version": self.candidate["version"]}

    def rollback(self) -> dict:
        self.reverted_version = rollback_mod.rollback()
        self.rolled_back = True
        return {"ok": True, "active_version": self.reverted_version}

    def finish(self, classification: str, resolution: str, root_cause: str, summary: str) -> dict:
        self.finish_info = {
            "classification": classification,
            "resolution": resolution,
            "root_cause": root_cause,
            "summary": summary,
        }
        return {"ok": True}


def _last_ok_before(runs: list[dict], current: dict) -> dict | None:
    idx = len(runs) - 1
    for i, entry in enumerate(runs):
        if entry is current:
            idx = i
            break
    for entry in reversed(runs[:idx]):
        if entry.get("status") == "ok":
            return entry
    return None


def _quarantine_sample(cycle_id: int, limit: int) -> list[dict]:
    try:
        with open(paths.quarantine_path(cycle_id), "r", encoding="utf-8") as f:
            return [json.loads(line) for line in f if line.strip()][:limit]
    except FileNotFoundError:
        return []


def _build_tool_functions(ctx: AgentToolContext) -> list:
    """Expose the context methods as annotated closures for Gemini's SDK to call."""

    def get_incident_report() -> dict:
        """Return the degraded cycle's metrics, reason codes, schema comparison, active config, and a sample of quarantined records. Call this first."""
        return ctx.get_incident_report()

    def add_field_alias(source_field: str, target_field: str) -> dict:
        """Map an incoming (drifted) field name to the expected canonical field name, to absorb a schema rename. source_field is the new name in the data; target_field is the required field name."""
        return ctx.add_field_alias(source_field, target_field)

    def set_field_nullable(field: str) -> dict:
        """Relax the null policy for a required field so null values are accepted instead of quarantined. Use for a null spike."""
        return ctx.set_field_nullable(field)

    def set_dedup_policy(policy: str) -> dict:
        """Set how duplicate primary keys are handled: 'drop' (discard re-deliveries) or 'quarantine'. Use 'drop' for duplicate-key incidents."""
        return ctx.set_dedup_policy(policy)

    def validate_candidate() -> dict:
        """Re-run the transform with the candidate config against the exact failing batch and report whether the quarantine rate returns to healthy. Must pass before promoting."""
        return ctx.validate_candidate()

    def promote_candidate() -> dict:
        """Promote the candidate config to active and re-run the cycle. Only succeeds if a validation has passed."""
        return ctx.promote_candidate()

    def rollback() -> dict:
        """Revert the active config to the last known-good version. Use when no safe fix exists or validation keeps failing."""
        return ctx.rollback()

    def finish(classification: str, resolution: str, root_cause: str, summary: str) -> dict:
        """Conclude the incident. classification: schema_drift|null_spike|duplicate_keys|unknown. resolution: patch_applied|rollback_executed. Provide root_cause and a concise summary."""
        return ctx.finish(classification, resolution, root_cause, summary)

    return [
        get_incident_report,
        add_field_alias,
        set_field_nullable,
        set_dedup_policy,
        validate_candidate,
        promote_candidate,
        rollback,
        finish,
    ]


def _extract_trace(history) -> list[dict]:
    trace: list[dict] = []
    for content in history or []:
        role = getattr(content, "role", "?")
        for part in getattr(content, "parts", []) or []:
            if getattr(part, "text", None):
                trace.append({"role": role, "type": "text", "text": part.text.strip()})
            fc = getattr(part, "function_call", None)
            if fc:
                trace.append(
                    {"role": role, "type": "tool_call", "name": fc.name, "args": dict(fc.args or {})}
                )
            fr = getattr(part, "function_response", None)
            if fr:
                trace.append(
                    {"role": role, "type": "tool_result", "name": fr.name, "response": dict(fr.response or {})}
                )
    return trace


def _infer_classification(diff: dict | None, rolled_back: bool) -> str:
    if diff:
        if "add_field_alias" in diff:
            return "schema_drift"
        if "add_nullable_field" in diff:
            return "null_spike"
        if "set_dedup_policy" in diff:
            return "duplicate_keys"
    return "unknown"


def _config_diff(active: dict, candidate: dict) -> dict | None:
    diff: dict = {}
    new_aliases = {
        k: v for k, v in candidate.get("field_aliases", {}).items()
        if k not in active.get("field_aliases", {})
    }
    if new_aliases:
        diff["add_field_alias"] = new_aliases
    new_nullable = [
        f for f in candidate.get("nullable_fields", [])
        if f not in active.get("nullable_fields", [])
    ]
    if new_nullable:
        diff["add_nullable_field"] = new_nullable
    if candidate.get("dedup_policy") != active.get("dedup_policy"):
        diff["set_dedup_policy"] = candidate.get("dedup_policy")
    return diff or None


class LLMUnavailable(RuntimeError):
    pass


def run_agent_llm(cycle_id: int, sink: Sink, tracker: Tracker) -> dict:
    """Run the Gemini tool-calling agent for a degraded cycle. Raises
    LLMUnavailable if Gemini can't be reached (caller should fall back)."""
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        raise LLMUnavailable("no Gemini API key")

    try:
        from google import genai
        from google.genai import types
    except ImportError as exc:
        raise LLMUnavailable("google-genai not installed") from exc

    ctx = AgentToolContext(cycle_id, sink, tracker)
    tools = _build_tool_functions(ctx)

    try:
        client = genai.Client(api_key=api_key)
        config = types.GenerateContentConfig(
            tools=tools,
            system_instruction=SYSTEM_INSTRUCTION,
            temperature=0,
            automatic_function_calling=types.AutomaticFunctionCallingConfig(
                maximum_remote_calls=MAX_TOOL_CALLS
            ),
        )
        model_name = os.environ.get("HOMEOSTAT_LLM_MODEL", MODEL_DEFAULT)
        resp = client.models.generate_content(
            model=model_name,
            contents=(
                f"Pipeline cycle {cycle_id} has been flagged DEGRADED. Investigate "
                f"and restore it to a healthy state using your tools. Begin by "
                f"calling get_incident_report."
            ),
            config=config,
        )
    except Exception as exc:  # pragma: no cover - network path
        raise LLMUnavailable(str(exc)) from exc

    trace = _extract_trace(getattr(resp, "automatic_function_calling_history", None))
    final_text = (getattr(resp, "text", None) or "").strip()

    # Safety net: if the model neither promoted a validated fix nor rolled back,
    # the pipeline would be left degraded -> force a rollback.
    if not ctx.promoted and not ctx.rolled_back:
        ctx.rollback()

    resolution = "patch_applied" if ctx.promoted else "rollback_executed"
    info = ctx.finish_info or {}
    config_after = config_store.load_active_config()
    diff = _config_diff(ctx.active_config, ctx.candidate) if ctx.promoted else None

    # Trust the actions actually taken over what the model claimed in finish():
    # weaker models sometimes end with prose instead of a structured finish call.
    inferred = _infer_classification(diff, ctx.rolled_back)
    classification = info.get("classification")
    known = ("schema_drift", "null_spike", "duplicate_keys", "unknown")
    if classification not in known or (classification == "unknown" and inferred != "unknown"):
        classification = inferred

    root_cause = info.get("root_cause")
    if not root_cause or root_cause == "n/a":
        root_cause = final_text or "(inferred from actions taken)"

    incident = {
        "cycle_id": cycle_id,
        "detected_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "classification": classification,
        "proposal": {
            "diagnosis_summary": info.get("summary") or final_text or "(no summary)",
            "root_cause": root_cause,
            "proposed_action": "patch_config" if ctx.promoted else "rollback",
            "narrative_source": f"gemini-agent ({os.environ.get('HOMEOSTAT_LLM_MODEL', MODEL_DEFAULT)})",
        },
        "config_diff": diff,
        "config_version_before": ctx.active_config["version"],
        "config_version_after": config_after["version"],
        "metrics_before": ctx.current["metrics"],
        "metrics_after": ctx.last_validation[1] if ctx.last_validation else None,
        "resolution": resolution,
        "agent_mode": "llm_tool_calling",
        "tool_calls": sum(1 for t in trace if t["type"] == "tool_call"),
        "agent_trace": trace,
        "outcome_note": info.get("summary") or final_text,
    }

    incident["postmortem_path"] = write_postmortem(incident, ctx.initial_sample)
    tracker.log_incident({k: v for k, v in incident.items() if k != "agent_trace"})
    return incident
