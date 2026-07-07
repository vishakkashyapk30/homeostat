# Homeostat — a self-healing data-pipeline agent

> **Homeostat** *(n.)* — a device that holds itself in a stable condition by
> sensing disturbances and automatically counteracting them. Coined by
> cyberneticist W. Ross Ashby in 1948 to describe a machine that returns itself
> to equilibrium no matter how it is perturbed.

Homeostat is a compact ETL pipeline (**ingest → transform → write**) wrapped in an
**autonomous agent** that continuously watches the pipeline's own health metrics,
**diagnoses data-quality failures, designs and validates a targeted fix, promotes
it to production, and writes an incident postmortem** — all without a human in the
loop, and with a **rollback safety net** for the cases it cannot safely fix.

In one sentence: *it is an on-call data engineer, encoded as software.*

The agent ships in two interchangeable forms:

- **LLM tool-calling agent** (default when a Gemini key is present) — **Google
  Gemini** is the decision-maker. It reasons over the incident and autonomously
  calls tools (investigate → patch → validate → promote / rollback → finish),
  observing each result and re-planning until the pipeline is healthy. Its full
  reasoning + tool-call trace is recorded in the postmortem.
- **Rule-based agent** — a deterministic control loop that resolves the same
  incidents with zero LLM calls. It is both a dependency-free default and the
  automatic fallback whenever the LLM is unavailable or rate-limited.

Either way, the *safety* lives in the tools (validate-before-promote, rollback),
not in trusting the model — so an LLM mistake can never corrupt state.

---

## Table of contents

1. [Genesis — why this exists](#genesis--why-this-exists)
2. [Why it matters](#why-it-matters)
3. [Practical use cases](#practical-use-cases)
4. [Architecture in detail](#architecture-in-detail)
5. [The agentic AI](#the-agentic-ai)
6. [The AI/LLM used](#the-aillm-used)
7. [Failure modes and remediations](#failure-modes-and-remediations)
8. [Installation](#installation)
9. [Command reference](#command-reference)
10. [Configuration reference](#configuration-reference)
11. [Output artifacts](#output-artifacts)
12. [Databricks / Lakehouse mode](#databricks--lakehouse-mode)
13. [MCP server](#mcp-server-drive-homeostat-from-any-mcp-client)
14. [Testing](#testing)
15. [Project layout](#project-layout)
16. [Extending Homeostat](#extending-homeostat)
17. [Design decisions & rationale](#design-decisions--rationale)

---

## Genesis — why this exists

Every data platform has the same 3 a.m. story. A pipeline that has run cleanly for
months suddenly starts dropping records. An engineer gets paged, opens dashboards,
scrolls logs, eventually realizes an upstream team renamed a column (or a service
started emitting nulls, or a producer began re-delivering messages), edits a
config, redeploys, and writes a postmortem the next morning. The *diagnosis* is
almost always drawn from the same small set of failure signatures, and the *fix*
is almost always a small, mechanical config change.

Homeostat began from a simple question: **if the triage is this repetitive, why is
a human doing it at 3 a.m.?** The failures are detectable from metrics, the fixes
are expressible as config diffs, and the safety checks (validate before promote,
roll back if wrong) are exactly what a careful engineer already does by hand.

The name is deliberate. Ashby's *homeostat* was one of the first machines that
could restore its own stability after being disturbed — a foundational idea in
cybernetics and, arguably, an ancestor of modern autonomous agents. Homeostat (the
project) applies that same closed-loop, self-regulating principle to a data
pipeline: **sense → diagnose → act → verify → stabilize.**

It is intentionally small enough to read end-to-end in an afternoon, but it models
the real control loop that production reliability systems are built on.

---

## Why it matters

Data reliability is now a first-class engineering concern, not an afterthought.
The industry even has a name for it — *data observability* / *data reliability
engineering* — and a whole category of tooling (Monte Carlo, Great Expectations,
Delta Live Tables expectations, dbt tests). Homeostat sits one step beyond
*detection*: it closes the loop with **automated remediation**.

- **Silent data corruption is expensive.** Bad data doesn't crash — it quietly
  poisons dashboards, ML features, and financial reports until someone notices
  downstream. Catching and quarantining it at ingestion is far cheaper than
  unwinding it later.
- **Mean-time-to-recovery (MTTR) dominates reliability.** Detection alone still
  needs a human to act. An agent that proposes and safely applies the fix
  collapses MTTR from hours to seconds for known failure classes.
- **Schema evolution is inevitable.** Upstream teams rename and retype fields.
  A pipeline that can absorb benign drift (via aliases) without a human is
  strictly more resilient.
- **Trust requires auditability.** Every automated action here is versioned,
  validated, logged, and explained in a postmortem — so an operator can always
  answer "what did the system do, and why?"

---

## Practical use cases

Homeostat is a reference design for any *event → validated store* pipeline. The
failure modes it handles are universal, so the same pattern maps onto many domains:

| Domain | What "self-healing" buys you |
|---|---|
| **Lakehouse / analytics ingestion** | Absorb upstream schema evolution and quarantine dirty rows automatically instead of failing a nightly job. |
| **Payments / fintech** | Detect duplicate-key re-deliveries (double-charge-style anomalies) and null spikes in transaction fields; drop idempotent retries safely. |
| **Ride-hailing / logistics event streams** | Keep high-volume GPS/trip-event ingestion healthy as producers change payloads. |
| **Clickstream / product analytics** | Handle renamed or added tracking fields without dropping a day of events. |
| **ML feature pipelines** | Prevent null/schema regressions from silently degrading model features; quarantine + alert instead. |
| **Data migrations & backfills** | Validate each batch against expectations and roll back a bad config version instantly. |

Because the pipeline core depends only on plain Python dicts and two abstract
interfaces (`Sink`, `Tracker`), the exact same agent runs on local files, a Delta
Lakehouse, or (with a new adapter) a cloud warehouse — see
[Databricks / Lakehouse mode](#databricks--lakehouse-mode).

---

## Architecture in detail

Homeostat is two cooperating subsystems: a **pipeline** that does the work and
records its own health, and an **agent** that reads that health and heals the
pipeline. They communicate through one artifact — the **run manifest** — which
keeps the agent completely decoupled from *how* the pipeline stores data.

```
                          ┌──────────────── one cycle ────────────────┐
                          │                                            │
  ingest.get_next_batch ──┤─▶ [failure_injector] ─▶ transform_batch ──┤─▶ Sink.write_clean
   (deterministic, seeded)│      (demo only)          (expectations)   │   Sink.write_quarantine
                          │                                │           │        (dead-letter)
                          │                                ▼           │
                          │                          compute_metrics   │
                          │                                │           │
                          │                                ▼           │
                          │                     Tracker.log_run ──▶ run_manifest.jsonl
                          └────────────────────────────────┼───────────┘
                                                            │  status == "degraded"?
                                                            ▼
        ┌──────────────────────────── SELF-HEALING AGENT ─────────────────────────────┐
        │  classifier.classify        rule-based label from metrics + last-good run     │
        │        │                     (schema_drift | null_spike | duplicate_keys |    │
        │        ▼                      unknown)                                        │
        │  diagnoser.diagnose         deterministic config_diff + (optional) LLM prose  │
        │        │                                                                      │
        │        ▼                                                                      │
        │  patch_executor.apply_diff  writes NEW transform_config_vN+1.json (never      │
        │        │                    mutates the active version)                       │
        │        ▼                                                                      │
        │  validator.validate         re-runs transform on the EXACT failing batch      │
        │        │                    with the candidate config                         │
        │   pass ┴ fail                                                                 │
        │    │        │                                                                 │
        │    ▼        ▼                                                                 │
        │ promote   rollback.rollback   ← safety net: revert active pointer to last     │
        │ (active     (last known-good)    healthy version                              │
        │  = vN+1)                                                                      │
        │    │        │                                                                 │
        │    ▼        ▼                                                                 │
        │  re-run    postmortem.write_postmortem ──▶ logs/incidents/incident_*.md       │
        │  cycle          │                                                             │
        │  (healed)       ▼                                                             │
        │            Tracker.log_incident                                               │
        └──────────────────────────────────────────────────────────────────────────────┘
```

### The pipeline (`src/`)

| Module | Responsibility |
|---|---|
| `ingest.py` | Produces deterministic synthetic **order-event** batches (`order_id, user_id, item_sku, quantity, price, timestamp, region`). Seeded by `base_seed + cycle_id` so every run is reproducible. In production this would wrap a Kafka/Kinesis consumer; the contract (`get_next_batch`) is unchanged. |
| `failure_injector.py` | Demo-only. Corrupts a batch in one of four controlled ways. Crucially, it **never records what it did in the manifest** — the agent must diagnose from metrics alone. |
| `transform.py` | The heart of the pipeline. Loads the active config and applies **expectations**: alias normalization → required-field check → null policy → type check → deduplication. Passing records go to `clean`; failures are routed to a dead-letter (`quarantine`) list with a **reason code**. |
| `metrics.py` | Computes per-cycle health: counts, `quarantine_rate`, `duplicate_rate`, `null_rate_per_field`, a normalized `schema_hash`, and `reason_counts`. |
| `orchestrator.py` | Runs one full cycle and decides `status`: **degraded** if `quarantine_rate` exceeds the threshold *or* the schema hash changed since the last healthy run; otherwise **ok**. |
| `config_store.py` | Reads/writes versioned configs and the `active_version.txt` pointer. |
| `manifest.py` | Append/read the JSONL run manifest — the single source of truth the agent consumes. |

### The adapters (`src/adapters/`)

The pipeline never imports a vendor SDK directly. Two small interfaces make the
storage and observability layers swappable:

- **`Sink`** — `write_clean()` and `write_quarantine()`.
  - `LocalSink` (default): SQLite table `clean_orders` + JSONL dead-letter files. Zero infra.
  - `DeltaSink` (optional): Delta Lake tables (`--backend delta`).
- **`Tracker`** — `log_run()` and `log_incident()`.
  - `LocalTracker` (default): appends to the JSONL manifest + an incidents index.
  - `MLflowTracker` (optional): every cycle becomes an MLflow run; incidents log the postmortem + config diff as artifacts (`--tracker mlflow`). It *also* writes the manifest, so the agent keeps working unchanged.

### The agent (`src/agent/`)

Covered in depth in the next section.

---

## The agentic AI

Homeostat implements a genuine **closed-loop autonomous agent** — a
sense–plan–act–verify cycle (an OODA loop, in military terms; a control loop, in
cybernetics terms). It ships in **two forms** that share the same tools and
guardrails.

### 1. LLM tool-calling agent (`--agent llm`, default when a key is present)

Here **Gemini is the decision-maker.** It is given a set of tools and must decide
which to call, observe each result, and re-plan until the pipeline is healthy.
This is agentic in the modern sense: *the model reasons and calls tools*, rather
than a script calling the model.

The tools exposed to the model (`src/agent/agentic.py`):

| Tool | What it does |
|---|---|
| `get_incident_report()` | Returns metrics, reason codes, schema comparison, active config, and a sample of quarantined records. |
| `add_field_alias(source, target)` | Maps a drifted field name back to the canonical one (schema-drift fix). |
| `set_field_nullable(field)` | Relaxes the null policy for a field (null-spike fix). |
| `set_dedup_policy(policy)` | Switches duplicate handling to `drop` (duplicate-key fix). |
| `validate_candidate()` | Re-runs the transform with the candidate config on the failing batch. |
| `promote_candidate()` | Promotes the fix and re-runs the cycle — **refuses unless a validation has passed.** |
| `rollback()` | Reverts to the last known-good config. |
| `finish(classification, resolution, root_cause, summary)` | Concludes the incident. |

A real decision trace produced by Gemini (recorded verbatim in the postmortem):

```
1. call get_incident_report()
   ↳ reason_counts: {"duplicate_key": 45}, quarantine_rate: 0.13
 > "The primary reason for quarantine is 'duplicate_key' (45 records)...
    I will set the deduplication policy to 'drop'."
2. call set_dedup_policy("drop")
3. call validate_candidate()   ↳ passed: true, quarantine_rate_after: 0.0
 > "The validation passed. I will now promote this candidate configuration."
4. call promote_candidate()    ↳ ok: true, active_version: 2
```

**The safety is in the tools, not the model.** `promote_candidate()` mechanically
refuses to promote a fix that hasn't passed `validate_candidate()`, and if the
agent ends without promoting or rolling back, the loop forces a rollback. So the
model plans freely, but it cannot corrupt state or ship an unproven fix.

### 2. Rule-based agent (`--agent rules`, and the automatic fallback)

A deterministic version of the exact same loop, with **no LLM calls**:

| Step | Module | What it does |
|---|---|---|
| **Classify** | `agent/classifier.py` | Returns `schema_drift`, `null_spike`, `duplicate_keys`, `unknown`, or `healthy` from the metrics (schema-hash change + dominant reason code ≥ 50%). |
| **Diagnose** | `agent/diagnoser.py` | Produces a `config_diff`; infers the alias for schema drift from the quarantined records. |
| **Patch / Validate / Promote / Rollback** | `patch_executor`, `validator`, `rollback` | Identical guardrails to the LLM tools. |
| **Explain** | `agent/postmortem.py` | Writes the incident postmortem. |
| **Orchestrate** | `agent/loop.py` | `run_agent(cycle_id, ...)`, invoked whenever a cycle is `degraded`. |

### Why keep a deterministic path at all?

Because it makes the system **reliable, testable, and free** for known failures,
and it is the **automatic safety fallback**: during development the Gemini free
tier rate-limited mid-run, and the pipeline kept self-healing without a hiccup by
switching to rules. That "LLM-driven, deterministically-backed" split is exactly
what you'd want in a real production autonomous system.

---

## The AI/LLM used

Homeostat uses **Google Gemini** (default model **`gemini-2.5-flash`**) via the
modern [`google-genai`](https://pypi.org/project/google-genai/) SDK, with native
**function/tool calling**.

**In `--agent llm` mode, Gemini drives the whole remediation:** it calls
`get_incident_report`, reasons about the failure, calls the appropriate fix tool,
validates, and promotes or rolls back — a real multi-step tool-use loop, not a
single prompt. Its reasoning and every tool call/result are captured in the
postmortem's **Agent Decision Trace**.

**Guardrails, not blind trust:** the tools enforce safety (no promotion without a
passing validation; forced rollback if the agent stops without resolving), so an
LLM error cannot corrupt data or ship a bad fix.

**Provider-agnostic & always-degradable.** Provider auto-selected:

- `GEMINI_API_KEY` (or `GOOGLE_API_KEY`) set → **Gemini** (tool-calling agent)
- neither key, or LLM unavailable/rate-limited → **rule-based agent** (identical outcome, no LLM)

Override with `HOMEOSTAT_LLM_MODEL` (e.g. `gemini-2.5-flash`,
`gemini-2.5-flash-lite`). Keys live in a git-ignored `.env` (see
[`.env.example`](.env.example)); **no key is ever committed**.

> **Rate limits:** the Gemini free tier is limited (e.g. ~20 requests/day for
> `gemini-2.5-flash`), and one tool-calling incident uses several requests. A
> single-incident demo showcases the LLM agent well; longer multi-incident runs
> will gracefully fall back to the rule-based agent when the quota is hit. Use a
> paid tier (or `--agent rules`) for fully deterministic multi-incident runs.

See a full agentic run in
[`examples/sample_incident_postmortem.md`](examples/sample_incident_postmortem.md),
including Gemini's verbatim reasoning and tool calls.

---

## Failure modes and remediations

| Injection | Detected as | Agent's remediation |
|---|---|---|
| `schema_drift` — upstream renames `item_sku` → `sku_code` | schema-hash change / `missing_field` dominant | adds a **field alias** so the drifted schema normalizes back to canonical (schema evolution, absorbed) |
| `null_spike` — ~45% of `user_id` become null | `null_violation` dominant | **relaxes the null policy** for the affected field |
| `duplicate_keys` — ~15% of records re-delivered | `duplicate_key` dominant | switches **dedup policy to `drop`** (treat as idempotent retries) |
| `type_drift` — `price` starts arriving as a string | `unknown` (unrecognized signature) | **rolls back** to the last known-good config — the safety net |

The first three demonstrate autonomous *repair*; the fourth demonstrates
autonomous *containment* when repair isn't safe.

---

## Installation

Requires **Python 3.10+**. The full demo runs on the **standard library alone** —
no installation required.

```bash
git clone https://github.com/vishakkashyapk30/homeostat.git
cd homeostat

# Run immediately — no dependencies needed:
python -m src.cli run --cycles 10 --inject 4:schema_drift --fresh
```

Optional extras (each unlocks one feature; nothing is required for the core demo):

```bash
pip install -r requirements.txt      # everything below, or install individually:
pip install google-genai             # Gemini-authored postmortem narratives
pip install openai                   # alternative LLM provider
pip install deltalake pyarrow        # --backend delta  (Delta Lake sink)
pip install mlflow                   # --tracker mlflow  (experiment tracking)
pip install pytest                   # run the test suite
```

---

## Command reference

The single entrypoint is `python -m src.cli`. It has two subcommands: `run` and `show`.

### `run` — execute pipeline cycles

```
python -m src.cli run [options]
```

| Flag | Default | Description |
|---|---|---|
| `--cycles N` | `10` | Number of pipeline cycles to run. |
| `--size N` | `300` | Records generated per batch. |
| `--seed N` | `42` | Base RNG seed; makes ingestion **and** injection fully reproducible. |
| `--inject CYCLE:TYPE` | — | Inject a failure into a specific cycle. **Repeatable.** `TYPE` ∈ `schema_drift`, `null_spike`, `duplicate_keys`, `type_drift`. |
| `--backend {local,delta}` | `local` | Storage sink. `delta` requires `deltalake` + `pyarrow`. |
| `--tracker {local,mlflow}` | `local` | Observability tracker. `mlflow` requires `mlflow`. |
| `--agent {auto,rules,llm}` | `auto` | Healing agent. `llm` = Gemini tool-calling; `rules` = deterministic; `auto` = llm if a Gemini key is present, else rules. |
| `--fresh` | off | Reset all state (manifest, incidents, generated configs, data) before running. |

### `show` — summarize the clean store

```
python -m src.cli show
```

Prints the number of rows currently in the `clean_orders` SQLite table.

### Ready-to-run examples

```bash
# 1. The canonical demo: three failures, three autonomous fixes.
python -m src.cli run --cycles 10 \
    --inject 4:schema_drift \
    --inject 7:null_spike \
    --inject 9:duplicate_keys \
    --fresh

# 2. Show ONLY the rollback safety net (an unfixable failure).
python -m src.cli run --cycles 5 --inject 3:type_drift --fresh

# 2b. Force the Gemini tool-calling agent for a single incident (see the trace).
python -m src.cli run --cycles 4 --inject 3:schema_drift --agent llm --fresh

# 2c. Force the deterministic rule-based agent (no LLM, fully reproducible).
python -m src.cli run --cycles 10 --inject 4:schema_drift --inject 7:null_spike \
    --inject 9:duplicate_keys --agent rules --fresh

# 3. A single failure type, larger batches.
python -m src.cli run --cycles 8 --size 1000 --inject 4:null_spike --fresh

# 4. Multiple failures of the same kind across a longer run.
python -m src.cli run --cycles 15 \
    --inject 3:schema_drift --inject 8:schema_drift --inject 12:duplicate_keys --fresh

# 5. A perfectly healthy run (baseline — no injections).
python -m src.cli run --cycles 6 --fresh

# 6. Reproducibility check — same seed ⇒ identical output.
python -m src.cli run --cycles 5 --inject 3:null_spike --seed 7 --fresh

# 7. Databricks / Lakehouse mode (needs the optional deps).
python -m src.cli run --cycles 10 --inject 4:schema_drift \
    --backend delta --tracker mlflow --fresh

# 8. With Gemini narratives (after adding a key to .env).
python -m src.cli run --cycles 5 --inject 3:schema_drift --fresh

# 9. Inspect results.
python -m src.cli show
cat logs/run_manifest.jsonl | tail -5
ls logs/incidents/

# 10. Run the tests.
python -m pytest -q
```

---

## Configuration reference

### Transform config (`config/transform_config_v1.json`)

This versioned JSON is the single source of truth the transform reads — and the
object the agent patches.

| Field | Meaning |
|---|---|
| `version` | Integer version of this config. |
| `primary_key` | Field that must be present and non-null (e.g. `order_id`). |
| `dedup_key` | Field used to detect duplicates. |
| `required_fields` | Fields that must exist in every record. |
| `field_types` | Expected type per field (`str`, `int`, `float`, `bool`). |
| `field_aliases` | Map of `incoming_name → canonical_name`; how schema drift is absorbed. |
| `nullable_fields` | Fields allowed to be null; how null spikes are tolerated. |
| `dedup_policy` | `quarantine` (route duplicates to dead-letter) or `drop` (silently discard retries). |
| `quarantine_threshold` | Fraction of quarantined records above which a cycle is `degraded` (default `0.05`). |

### Environment variables (`.env`)

| Variable | Purpose |
|---|---|
| `GEMINI_API_KEY` / `GOOGLE_API_KEY` | Enable Gemini narratives. |
| `OPENAI_API_KEY` | Enable OpenAI narratives (fallback provider). |
| `HOMEOSTAT_LLM_PROVIDER` | Force `gemini` or `openai`. |
| `HOMEOSTAT_LLM_MODEL` | Override the model (e.g. `gemini-2.5-flash`). |

Copy `.env.example` → `.env` and fill in a key. `.env` is git-ignored.

---

## Output artifacts

Every run produces inspectable evidence:

| Path | Contents |
|---|---|
| `logs/run_manifest.jsonl` | One JSON line per cycle: status, config version, and full metrics. The agent's source of truth. |
| `logs/incidents/incident_*.md` | A human-readable postmortem per incident (diagnosis, evidence, action, outcome). |
| `logs/incidents_index.jsonl` | Machine-readable index of incident resolutions. |
| `data/clean/cycle_*.jsonl` + `data/clean.db` | Successfully transformed records. |
| `data/quarantine/cycle_*.jsonl` | Rejected records, each tagged with a reason code. |
| `data/raw/cycle_*.jsonl` | The exact (post-injection) input batch — this is what the validator replays. |
| `config/transform_config_v*.json` | Every config version the agent has produced (auditable history). |

A committed worked example lives in
[`examples/sample_incident_postmortem.md`](examples/sample_incident_postmortem.md)
and [`examples/sample_run_manifest.jsonl`](examples/sample_run_manifest.jsonl).

---

## Databricks / Lakehouse mode

The same pipeline runs on Databricks-native tooling by swapping adapters — the
pipeline and agent logic are untouched:

```bash
pip install deltalake pyarrow mlflow
python -m src.cli run --cycles 10 --inject 4:schema_drift \
    --backend delta \    # clean + dead-letter records → Delta Lake tables
    --tracker mlflow     # each cycle + incident → an MLflow run
```

- **`--backend delta`** writes to [Delta Lake](https://delta.io/) tables (created by Databricks).
- **`--tracker mlflow`** logs each cycle to [MLflow](https://mlflow.org/) with params
  (config version), metrics (quarantine/null/dup rates), and status tags; each
  incident additionally logs the postmortem and `config_diff` as artifacts.
- The data-quality checks intentionally use the vocabulary of **expectations**,
  matching Delta Live Tables and Great Expectations.

---

## MCP server (drive Homeostat from any MCP client)

Homeostat ships an [MCP](https://modelcontextprotocol.io/) server that exposes
the pipeline and the **same guardrailed healing tools** the in-process Gemini
agent uses. This means any MCP client — Claude Desktop, Cursor, or your own —
can *be* the healing agent: run the pipeline, inspect an incident, apply a fix,
validate, and promote, all over the protocol.

```bash
pip install mcp
python -m src.mcp_server        # stdio transport
```

### Tools exposed

| Tool | Purpose |
|---|---|
| `run_pipeline(cycles, inject, auto_heal, fresh)` | Run cycles; optionally inject failures and/or auto-heal. |
| `get_pipeline_status(limit)` | Recent manifest entries + which cycles are degraded. |
| `begin_incident(cycle_id)` | Open a healing session; returns the incident report. |
| `add_field_alias`, `set_field_nullable`, `set_dedup_policy` | Propose a fix on the candidate config. |
| `validate_candidate()` | Re-run the transform on the failing batch with the candidate. |
| `promote_candidate()` | Promote — **refuses unless a validation has passed.** |
| `rollback()` | Revert to the last known-good config. |
| `get_active_config`, `list_incidents`, `read_postmortem` | Inspect state and reports. |

The guardrails live in the tools, so even an external client cannot promote an
unvalidated fix.

### Register it with a client

**Cursor** (`.cursor/mcp.json`) or **Claude Desktop**
(`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "homeostat": {
      "command": "python",
      "args": ["-m", "src.mcp_server"],
      "cwd": "/absolute/path/to/homeostat"
    }
  }
}
```

Then ask the client, e.g.: *"Run the pipeline with a schema_drift on cycle 3,
then investigate and heal it."* — it will call `run_pipeline`, `begin_incident`,
`add_field_alias`, `validate_candidate`, and `promote_candidate` on its own.

---

## Testing

```bash
pip install pytest
python -m pytest -q
```

The suite (`tests/`) covers the transform's expectation enforcement and dedup
policies, alias-based schema absorption, deterministic failure injection, and
every classifier decision path.

---

## Project layout

```
homeostat/
├── config/
│   ├── transform_config_v1.json     # versioned baseline config (the agent patches this)
│   └── active_version.txt           # pointer to the currently live config version
├── data/                            # runtime outputs (git-ignored): raw / clean / quarantine
├── logs/
│   ├── run_manifest.jsonl           # one line per cycle — the agent's source of truth
│   └── incidents/                   # generated postmortems
├── src/
│   ├── ingest.py                    # deterministic synthetic event source
│   ├── failure_injector.py          # controlled corruption (demo only)
│   ├── transform.py                 # applies data-quality expectations
│   ├── metrics.py                   # per-cycle health metrics
│   ├── orchestrator.py              # runs one cycle, decides ok/degraded
│   ├── config_store.py              # versioned config read/write + active pointer
│   ├── manifest.py                  # run-manifest read/append
│   ├── env.py                       # dependency-free .env loader
│   ├── cli.py                       # entrypoint
│   ├── mcp_server.py                # MCP server (drive the pipeline from any MCP client)
│   ├── adapters/
│   │   ├── sink.py / sink_local.py / sink_delta.py
│   │   └── tracker.py / tracker_local.py / tracker_mlflow.py
│   └── agent/
│       ├── agentic.py               # Gemini tool-calling agent (LLM is decision-maker)
│       ├── classifier.py            # rule-based failure classification
│       ├── diagnoser.py             # structured fix proposal (+ optional LLM prose)
│       ├── llm.py                   # Gemini / OpenAI provider (narrative)
│       ├── patch_executor.py        # writes new config version
│       ├── validator.py             # validate-before-promote
│       ├── rollback.py              # safety net
│       ├── postmortem.py            # markdown incident reports (incl. decision trace)
│       └── loop.py                  # wires the rule-based agent pipeline
├── tests/                           # unit tests
├── examples/                        # a committed sample manifest + postmortem
├── plan.md                          # the original design/build plan
├── requirements.txt                 # optional dependencies
└── .env.example                     # LLM key template (copy to .env)
```

---

## Extending Homeostat

- **A new failure class:** add an injector in `failure_injector.py`, a detection
  branch in `classifier.py`, and a fix proposal in `diagnoser.py`.
- **A new storage backend:** implement the `Sink` interface (see `sink_local.py`)
  and register it in `cli.py`. The pipeline needs no other changes — that's the
  whole point of the adapter design.
- **A new observability backend:** implement `Tracker` (see `tracker_mlflow.py`).
- **A new LLM provider:** add a branch in `agent/llm.py`; the fix logic is
  unaffected because the LLM only writes narrative.

---

## Design decisions & rationale

| Decision | Why |
|---|---|
| **Two agents sharing one set of guardrailed tools** | The LLM agent plans; the rule-based agent is a deterministic, testable, always-available fallback. Same safety, same outcomes. |
| **Safety lives in the tools, not the model** | `promote` refuses an unvalidated fix; the loop forces a rollback if unresolved — so an LLM mistake cannot corrupt state or ship a bad fix. |
| **Versioned configs, never mutated in place** | Full audit trail and cheap, instant rollback. |
| **Validate before promote** | An automated fix is proven on the exact failing batch before going live — mirrors canary/shadow deployment. |
| **Rollback safety net** | Bounds the blast radius of an incorrect automated fix — the single most important property of any self-healing system. |
| **Generic core, pluggable backends** | The same agent runs on local files, a Lakehouse, or a warehouse by swapping one adapter — no vendor lock-in. |
| **The agent reads only the manifest** | It is fully decoupled from storage internals and can never "cheat" by reading what the injector did. |
