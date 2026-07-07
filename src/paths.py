"""Canonical filesystem locations for the project.

Everything else resolves paths through here so the layout stays in one place.
"""

import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CONFIG_DIR = os.path.join(ROOT, "config")
ACTIVE_VERSION_PATH = os.path.join(CONFIG_DIR, "active_version.txt")

DATA_DIR = os.path.join(ROOT, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
CLEAN_DIR = os.path.join(DATA_DIR, "clean")
QUARANTINE_DIR = os.path.join(DATA_DIR, "quarantine")
CLEAN_DB_PATH = os.path.join(DATA_DIR, "clean.db")

LOGS_DIR = os.path.join(ROOT, "logs")
INCIDENTS_DIR = os.path.join(LOGS_DIR, "incidents")
MANIFEST_PATH = os.path.join(LOGS_DIR, "run_manifest.jsonl")
INCIDENTS_INDEX_PATH = os.path.join(LOGS_DIR, "incidents_index.jsonl")


def config_path(version: int) -> str:
    return os.path.join(CONFIG_DIR, f"transform_config_v{version}.json")


def raw_path(cycle_id: int) -> str:
    return os.path.join(RAW_DIR, f"cycle_{cycle_id:03d}.jsonl")


def clean_path(cycle_id: int) -> str:
    return os.path.join(CLEAN_DIR, f"cycle_{cycle_id:03d}.jsonl")


def quarantine_path(cycle_id: int) -> str:
    return os.path.join(QUARANTINE_DIR, f"cycle_{cycle_id:03d}.jsonl")


def ensure_dirs() -> None:
    for d in (CONFIG_DIR, RAW_DIR, CLEAN_DIR, QUARANTINE_DIR, LOGS_DIR, INCIDENTS_DIR):
        os.makedirs(d, exist_ok=True)
