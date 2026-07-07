"""The safety net: revert the active config to the last known-good version.

This bounds the blast radius of an incorrect automated fix -- the single most
important design decision in any self-healing system.
"""

from .. import config_store, manifest


def last_good_version(default: int = 1) -> int:
    for entry in reversed(manifest.read_runs()):
        if entry.get("status") == "ok":
            return entry.get("transform_config_version", default)
    return default


def rollback() -> int:
    version = last_good_version()
    config_store.set_active_version(version)
    return version
