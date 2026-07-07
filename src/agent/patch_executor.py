"""Mechanically applies a config_diff to produce a NEW versioned config.

Never mutates the active config in place -- always versions up. The new version
is written to disk but is NOT promoted to active until the validator approves it.
"""

import copy

from .. import config_store


def apply_diff(config: dict, diff: dict) -> dict:
    new_config = copy.deepcopy(config)
    new_config["version"] = config["version"] + 1

    if "add_field_alias" in diff:
        new_config.setdefault("field_aliases", {}).update(diff["add_field_alias"])

    if "add_nullable_field" in diff:
        field = diff["add_nullable_field"]
        nullable = new_config.setdefault("nullable_fields", [])
        if field is not None and field not in nullable:
            nullable.append(field)

    if "set_dedup_policy" in diff:
        new_config["dedup_policy"] = diff["set_dedup_policy"]

    return new_config


def write_new_version(new_config: dict) -> int:
    config_store.save_config(new_config)
    return new_config["version"]
