"""Minimal, dependency-free .env loader.

Loads KEY=VALUE pairs from a project-root .env into os.environ without
overwriting variables that are already set. Kept tiny on purpose so the core
project needs zero third-party packages.
"""

import os

from .paths import ROOT


def load_dotenv(path: str | None = None) -> None:
    path = path or os.path.join(ROOT, ".env")
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value
