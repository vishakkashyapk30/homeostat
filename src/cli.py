"""Homeostat CLI: run N pipeline cycles, injecting failures on chosen cycles.

Examples:
    python -m src.cli run --cycles 10 --inject 4:schema_drift --inject 7:null_spike --fresh
    python -m src.cli run --cycles 8 --inject 3:duplicate_keys --backend delta --tracker mlflow
"""

import argparse
import glob
import os
import sqlite3

from . import config_store, paths
from .agent.loop import run_agent
from .env import load_dotenv
from .failure_injector import FAILURE_TYPES, inject
from .ingest import get_next_batch
from .orchestrator import run_cycle


def _make_sink(name: str):
    if name == "delta":
        from .adapters.sink_delta import DeltaSink

        return DeltaSink()
    from .adapters.sink_local import LocalSink

    return LocalSink()


def _make_tracker(name: str):
    if name == "mlflow":
        from .adapters.tracker_mlflow import MLflowTracker

        return MLflowTracker()
    from .adapters.tracker_local import LocalTracker

    return LocalTracker()


def _parse_inject(values: list[str]) -> dict:
    inject_map: dict = {}
    for spec in values or []:
        try:
            cycle_str, ftype = spec.split(":", 1)
            cycle = int(cycle_str)
        except ValueError:
            raise SystemExit(f"Invalid --inject spec {spec!r}. Expected '<cycle>:<failure_type>'.")
        if ftype not in FAILURE_TYPES:
            raise SystemExit(f"Unknown failure type {ftype!r}. Expected one of {FAILURE_TYPES}.")
        inject_map[cycle] = ftype
    return inject_map


def _reset_state() -> None:
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
        base = os.path.basename(f)
        if base != "transform_config_v1.json":
            os.remove(f)
    config_store.set_active_version(1)


def _fmt_status(entry: dict) -> str:
    m = entry["metrics"]
    tag = "OK " if entry["status"] == "ok" else "DEG"
    heal = " (healed)" if entry.get("post_heal") else ""
    return (
        f"[{tag}] cycle {entry['cycle_id']:03d} v{entry['transform_config_version']}{heal} | "
        f"clean={m['clean_count']} quarantine={m['quarantine_count']} "
        f"deduped={m['deduped_count']} q_rate={m['quarantine_rate']} "
        f"schema={m['schema_hash']}"
    )


def _cmd_run(args: argparse.Namespace) -> int:
    paths.ensure_dirs()
    if args.fresh:
        _reset_state()

    inject_map = _parse_inject(args.inject)
    sink = _make_sink(args.backend)
    tracker = _make_tracker(args.tracker)

    print(f"Homeostat run: {args.cycles} cycles | backend={sink.name()} tracker={tracker.name()}")
    print("-" * 78)

    for cycle_id in range(1, args.cycles + 1):
        config = config_store.load_active_config()
        batch = get_next_batch(cycle_id, size=args.size, base_seed=args.seed)

        ftype = inject_map.get(cycle_id)
        if ftype:
            print(f"  >> injecting '{ftype}' into cycle {cycle_id:03d}")
            batch = inject(batch, ftype, cycle_id, base_seed=args.seed)

        entry = run_cycle(cycle_id, batch, config, sink, tracker)
        print(_fmt_status(entry))

        if entry["status"] == "degraded":
            print("     -> degraded detected; invoking self-healing agent...")
            incident = run_agent(cycle_id, sink, tracker)
            print(
                f"     -> classification={incident['classification']} "
                f"resolution={incident['resolution']} "
                f"config v{incident['config_version_before']}->v{incident['config_version_after']}"
            )
            print(f"     -> postmortem: {os.path.relpath(incident['postmortem_path'], paths.ROOT)}")

    print("-" * 78)
    print(f"Manifest: {os.path.relpath(paths.MANIFEST_PATH, paths.ROOT)}")
    print(f"Active config version: v{config_store.get_active_version()}")
    return 0


def _cmd_show(args: argparse.Namespace) -> int:
    con = sqlite3.connect(paths.CLEAN_DB_PATH)
    try:
        total = con.execute("SELECT COUNT(*) FROM clean_orders").fetchone()[0]
        print(f"clean_orders rows: {total}")
    finally:
        con.close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="homeostat", description="Self-healing ETL pipeline agent.")
    sub = parser.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser("run", help="Run N pipeline cycles with optional failure injection.")
    run_p.add_argument("--cycles", type=int, default=10, help="Number of cycles to run.")
    run_p.add_argument("--size", type=int, default=300, help="Records per batch.")
    run_p.add_argument("--seed", type=int, default=42, help="Base RNG seed (reproducible).")
    run_p.add_argument(
        "--inject",
        action="append",
        default=[],
        metavar="CYCLE:TYPE",
        help="Inject a failure, e.g. 4:schema_drift (repeatable).",
    )
    run_p.add_argument("--backend", choices=["local", "delta"], default="local", help="Sink backend.")
    run_p.add_argument("--tracker", choices=["local", "mlflow"], default="local", help="Tracker backend.")
    run_p.add_argument("--fresh", action="store_true", help="Reset all state before running.")
    run_p.set_defaults(func=_cmd_run)

    show_p = sub.add_parser("show", help="Show a quick summary of the clean store.")
    show_p.set_defaults(func=_cmd_show)

    return parser


def main(argv=None) -> int:
    load_dotenv()
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
