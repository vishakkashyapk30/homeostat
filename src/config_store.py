"""Read/write access to versioned transform configs.

Configs are never mutated in place. Each patch produces a new
``transform_config_v{N+1}.json`` file, and a single pointer file
(``active_version.txt``) records which version is currently live. This gives
auditability and cheap rollback.
"""

import glob
import json
import os
import re

from . import paths


def get_active_version() -> int:
    with open(paths.ACTIVE_VERSION_PATH, "r", encoding="utf-8") as f:
        return int(f.read().strip())


def set_active_version(version: int) -> None:
    with open(paths.ACTIVE_VERSION_PATH, "w", encoding="utf-8") as f:
        f.write(f"{version}\n")


def load_config(version: int) -> dict:
    with open(paths.config_path(version), "r", encoding="utf-8") as f:
        return json.load(f)


def load_active_config() -> dict:
    return load_config(get_active_version())


def save_config(config: dict) -> str:
    version = config["version"]
    path = paths.config_path(version)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)
        f.write("\n")
    return path


def max_version() -> int:
    pattern = os.path.join(paths.CONFIG_DIR, "transform_config_v*.json")
    versions = []
    for p in glob.glob(pattern):
        m = re.search(r"transform_config_v(\d+)\.json$", os.path.basename(p))
        if m:
            versions.append(int(m.group(1)))
    return max(versions) if versions else 1
