# Homeostat — a self-healing data-pipeline agent

> Named after [Ashby's *homeostat*](https://en.wikipedia.org/wiki/Homeostat), the classic
> cybernetic machine that automatically returns itself to a stable state.

Homeostat is a small ETL pipeline (ingest → transform → write) plus an autonomous
agent that **watches the pipeline's own metrics, diagnoses data-quality failures,
proposes and validates a targeted fix, promotes it, and writes a postmortem —
with a rollback safety net if its own fix is wrong.** No human in the loop.

It's built to demonstrate the thing production data platforms actually care about:
**reliability under schema evolution and dirty data**, with full lifecycle
ownership (design → build → test → operate).

```
ingest ─▶ [failure injector] ─▶ transform ─▶ write (clean + dead-letter)
                                                 │
                                                 ▼
                                        metrics + run manifest
                                                 │
                        status == degraded?      ▼
        ┌───────────────────────────────────────────────────────┐
        │  AGENT:  classify ─▶ diagnose ─▶ patch (new config vN+1)│
        │          ─▶ validate on the failing batch               │
        │          ─▶ promote  ✔   or   rollback  ✘               │
        │          ─▶ postmortem.md                               │
        └───────────────────────────────────────────────────────┘
```

## Why this design

| Decision | Rationale |
|---|---|
| **Rule-based classifier before any LLM** | Deterministic, fast, free, and reliable for known failure modes. The LLM is reserved for human-readable narrative and the `unknown` catch-all, so the system heals correctly even with no API key. |
| **Versioned configs, never mutated in place** | Auditability and cheap rollback. Every fix is a new `transform_config_vN+1.json`. |
| **Validate before promote** | An automated fix is re-run against the *exact batch that failed*; it's promoted only if the quarantine rate returns to healthy. Mirrors canary/shadow deployment safety. |
| **Rollback safety net** | Bounds the blast radius of an incorrect automated fix — the single most important property of any "self-healing" system. |
| **Generic core, pluggable backends** | The pipeline depends only on plain dicts + abstract `Sink`/`Tracker` interfaces, so the same agent runs on local files, a Lakehouse, or a warehouse by swapping one adapter. |

## Quick start (zero dependencies)

The full demo runs on the **Python standard library alone** — nothing to install.

```bash
python -m src.cli run --cycles 10 \
    --inject 4:schema_drift \
    --inject 7:null_spike \
    --inject 9:duplicate_keys \
    --fresh
```

Example output:

```
[OK ] cycle 003 v1 | clean=300 quarantine=0   deduped=0  q_rate=0.0    schema=a0b680ad5d46
  >> injecting 'schema_drift' into cycle 004
[DEG] cycle 004 v1 | clean=0   quarantine=300 deduped=0  q_rate=1.0    schema=ba638cd0d87e
     -> degraded detected; invoking self-healing agent...
     -> classification=schema_drift resolution=patch_applied config v1->v2
     -> postmortem: logs/incidents/incident_cycle_004_...md
[OK ] cycle 005 v2 | clean=300 quarantine=0   deduped=0  q_rate=0.0    schema=a0b680ad5d46
```

Every degraded cycle self-corrects within the same run, and later cycles return
to `ok` automatically. See a real generated postmortem in
[`examples/sample_incident_postmortem.md`](examples/sample_incident_postmortem.md)
and a full run manifest in [`examples/sample_run_manifest.jsonl`](examples/sample_run_manifest.jsonl).

## The three healable failures (and one that triggers rollback)

| Injection | Detected as | Fix the agent applies |
|---|---|---|
| `schema_drift` (`item_sku` → `sku_code`) | schema hash change / `missing_field` | adds a field **alias** so the drifted schema is absorbed (schema evolution) |
| `null_spike` (nulls in `user_id`) | `null_violation` dominant | **relaxes the null policy** for that field |
| `duplicate_keys` (re-delivered primary keys) | `duplicate_key` dominant | switches **dedup policy to drop** idempotent retries |
| `type_drift` (`price` becomes a string) | `unknown` | **rolls back** to the last known-good config (safety net) |

Try the safety net directly:

```bash
python -m src.cli run --cycles 5 --inject 3:type_drift --fresh
```

## Databricks / Lakehouse mode (optional)

The same demo can run on Databricks-native tooling by swapping adapters — the
pipeline logic is untouched:

```bash
pip install deltalake pyarrow mlflow
python -m src.cli run --cycles 10 --inject 4:schema_drift \
    --backend delta \    # clean + dead-letter records -> Delta Lake tables
    --tracker mlflow     # every cycle + incident -> an MLflow run (params, metrics, artifacts)
```

- **`--backend delta`** writes to [Delta Lake](https://delta.io/) tables (created by Databricks).
- **`--tracker mlflow`** logs each cycle as an [MLflow](https://mlflow.org/) run and each
  incident's postmortem + config diff as artifacts (MLflow is also a Databricks product).
- The data-quality checks use the vocabulary of *expectations*, matching
  Delta Live Tables / Great Expectations.

## Optional: LLM-authored narratives

The diagnoser can use an LLM to write the root-cause / diagnosis prose in each
postmortem. The **fix itself is always computed deterministically** — the LLM
never decides the config change — so behavior is identical with or without a key.

```bash
cp .env.example .env      # then add your key (.env is git-ignored)
pip install google-genai  # or: pip install openai
```

The provider is auto-selected: **Gemini** if `GEMINI_API_KEY` is set (default
model `gemini-2.5-flash`), otherwise **OpenAI** if `OPENAI_API_KEY` is set. Force
one with `HOMEOSTAT_LLM_PROVIDER=gemini|openai` and pick a model with
`HOMEOSTAT_LLM_MODEL`. When a key is present, postmortems show
`Narrative Source: gemini` (or `openai`); otherwise `deterministic`.

## Project layout

```
config/    versioned transform configs + active_version.txt pointer
src/       pipeline core (ingest, failure_injector, transform, metrics, write, orchestrator)
src/adapters/  Sink + Tracker interfaces and local / Delta / MLflow backends
src/agent/     classifier, diagnoser, patch_executor, validator, rollback, postmortem, loop
tests/     unit tests for transform, failure injector, and classifier
examples/  a committed sample run manifest + postmortem
plan.md    the original design/build plan
```

## Tests

```bash
pip install pytest
python -m pytest -q
```

## Demo script (for a walkthrough)

1. Run the baseline command above — show healthy cycles in the manifest.
2. Point out cycle 004 going `degraded` when schema drift is injected.
3. Show the agent classifying it, proposing a config patch, validating it on the
   failing batch, and promoting `v2` — all within the same run.
4. Open the generated postmortem and walk through evidence → action → outcome.
5. Repeat for `null_spike` / `duplicate_keys` to show generality.
6. Run the `type_drift` command to show the **rollback safety net** for a failure
   the agent can't safely auto-fix — i.e. "what if your agent's fix is wrong?"
