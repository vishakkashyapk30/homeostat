"""Turns a classification into a structured, machine-applyable fix proposal.

The `config_diff` is ALWAYS produced deterministically, so the system heals
correctly with zero external dependencies. When an OpenAI key is available the
diagnosis/root-cause *narrative* is enriched by an LLM, but the LLM never
decides the actual diff -- keeping the demo reliable even if the LLM is flaky.
"""

from ..transform import normalize_record
from . import llm


def _proposal(classification, root_cause, action, diff, summary):
    return {
        "classification": classification,
        "diagnosis_summary": summary,
        "root_cause": root_cause,
        "proposed_action": action,  # patch_config | rollback | retry_with_backoff
        "config_diff": diff,
    }


def _infer_alias(config: dict, sample: list[dict]):
    required = config["required_fields"]
    known = set(config.get("field_types", {}).keys())
    aliases = config.get("field_aliases", {})
    for item in sample:
        if item.get("reason") != "missing_field":
            continue
        rec = item["record"]
        keys = set(rec.keys())
        missing = [f for f in required if f not in normalize_record(rec, aliases)]
        extra = [k for k in keys if k not in known and k not in aliases]
        if missing and extra:
            return extra[0], missing[0]
    return None


def _worst_null_field(config: dict, current: dict):
    nullable = set(config.get("nullable_fields", []))
    rates = current["metrics"].get("null_rate_per_field", {})
    candidates = {f: r for f, r in rates.items() if f not in nullable}
    if not candidates:
        return None
    return max(candidates, key=candidates.get)


def diagnose(classification: str, config: dict, current: dict, sample: list[dict]) -> dict:
    if classification == "schema_drift":
        alias = _infer_alias(config, sample)
        if alias:
            src, dst = alias
            base = _proposal(
                classification,
                root_cause=f"Upstream renamed field '{dst}' to '{src}' (schema drift).",
                action="patch_config",
                diff={"add_field_alias": {src: dst}},
                summary=f"Schema drift detected: '{dst}' is arriving as '{src}'. "
                f"Add an alias so the drifted schema is absorbed safely.",
            )
        else:
            base = _proposal(
                classification,
                root_cause="Schema drift detected but no safe alias could be inferred.",
                action="rollback",
                diff=None,
                summary="Schema drift with no inferable alias; rolling back to last known-good config.",
            )
    elif classification == "null_spike":
        field = _worst_null_field(config, current)
        base = _proposal(
            classification,
            root_cause=f"Field '{field}' experienced a spike in null values.",
            action="patch_config",
            diff={"add_nullable_field": field},
            summary=f"Null spike on '{field}'. Relax its null policy to accept nulls "
            f"rather than quarantining otherwise-valid records.",
        )
    elif classification == "duplicate_keys":
        base = _proposal(
            classification,
            root_cause="Records re-delivered with duplicate primary keys (idempotent retries).",
            action="patch_config",
            diff={"set_dedup_policy": "drop"},
            summary="Duplicate keys detected. Switch dedup policy to drop exact "
            "re-deliveries instead of quarantining them.",
        )
    else:  # unknown / healthy
        base = _proposal(
            classification,
            root_cause="Degradation did not match a known failure signature.",
            action="rollback",
            diff=None,
            summary="Unrecognized failure pattern; rolling back to last known-good config as a safe default.",
        )

    return _maybe_enrich_with_llm(base, config, current, sample)


def _maybe_enrich_with_llm(base: dict, config: dict, current: dict, sample: list[dict]) -> dict:
    context = {
        "classification": base["classification"],
        "proposed_action": base["proposed_action"],
        "config_diff": base["config_diff"],
        "metrics": current.get("metrics"),
        "sample_quarantine": sample[:5],
    }
    enriched = llm.enrich_narrative(context)
    if not enriched:
        base["narrative_source"] = "deterministic"
        return base
    if enriched.get("root_cause"):
        base["root_cause"] = enriched["root_cause"]
    if enriched.get("diagnosis_summary"):
        base["diagnosis_summary"] = enriched["diagnosis_summary"]
    base["narrative_source"] = enriched.get("source", "llm")
    return base
