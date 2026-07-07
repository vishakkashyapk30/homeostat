"""Renders a human-readable markdown postmortem per incident."""

import datetime
import json
import os

from .. import paths


def _fmt_metrics(metrics: dict | None) -> str:
    if not metrics:
        return "_n/a_"
    lines = [
        f"- total_records: {metrics.get('total_records')}",
        f"- clean_count: {metrics.get('clean_count')}",
        f"- quarantine_count: {metrics.get('quarantine_count')}",
        f"- quarantine_rate: {metrics.get('quarantine_rate')}",
        f"- deduped_count: {metrics.get('deduped_count')}",
        f"- duplicate_rate: {metrics.get('duplicate_rate')}",
        f"- schema_hash: `{metrics.get('schema_hash')}`",
        f"- reason_counts: `{json.dumps(metrics.get('reason_counts', {}))}`",
    ]
    return "\n".join(lines)


def write_postmortem(incident: dict, sample: list[dict]) -> str:
    paths.ensure_dirs()
    ts = datetime.datetime.now(datetime.timezone.utc)
    cycle_id = incident["cycle_id"]
    fname = f"incident_cycle_{cycle_id:03d}_{ts.strftime('%Y%m%dT%H%M%SZ')}.md"
    path = os.path.join(paths.INCIDENTS_DIR, fname)

    proposal = incident.get("proposal", {})
    diff = incident.get("config_diff")
    sample_record = sample[0] if sample else None

    content = f"""# Incident Postmortem — Cycle {cycle_id:03d}

**Detected At:** {incident.get("detected_at")}
**Failure Classification:** `{incident.get("classification")}`
**Resolution Status:** `{incident.get("resolution")}`
**Config Version:** v{incident.get("config_version_before")} -> v{incident.get("config_version_after")}
**Narrative Source:** {proposal.get("narrative_source", "deterministic")}

## Diagnosis
{proposal.get("diagnosis_summary", "n/a")}

**Root Cause:** {proposal.get("root_cause", "n/a")}

## Evidence

### Metrics before
{_fmt_metrics(incident.get("metrics_before"))}

### Metrics after
{_fmt_metrics(incident.get("metrics_after"))}

### Sample quarantined record
```json
{json.dumps(sample_record, indent=2) if sample_record else "n/a"}
```

## Action Taken
**Proposed action:** `{proposal.get("proposed_action")}`

**Config diff applied:**
```json
{json.dumps(diff, indent=2) if diff is not None else "none (rollback / no patch)"}
```

## Outcome
{incident.get("outcome_note", "")}
"""

    trace_section = _fmt_trace(incident)
    if trace_section:
        content += trace_section

    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    return path


def _fmt_trace(incident: dict) -> str:
    trace = incident.get("agent_trace")
    if not trace:
        return ""
    lines = [
        "",
        "## Agent Decision Trace",
        f"_Mode: {incident.get('agent_mode', 'n/a')} · "
        f"tool calls: {incident.get('tool_calls', 0)}_",
        "",
    ]
    step = 0
    for item in trace:
        if item["type"] == "text":
            if item.get("role") == "model" and item.get("text"):
                lines.append(f"> {item['text']}")
                lines.append("")
        elif item["type"] == "tool_call":
            step += 1
            args = json.dumps(item.get("args", {}))
            lines.append(f"{step}. **call** `{item['name']}({args})`")
        elif item["type"] == "tool_result":
            resp = json.dumps(item.get("response", {}))
            snippet = resp if len(resp) <= 300 else resp[:297] + "..."
            lines.append(f"   ↳ result: `{snippet}`")
    lines.append("")
    return "\n".join(lines)
