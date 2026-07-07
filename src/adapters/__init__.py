"""Pluggable storage (`Sink`) and observability (`Tracker`) backends.

The pipeline core depends only on the abstract interfaces in `sink.py` and
`tracker.py`. Concrete backends (local files, Delta Lake, MLflow) are swapped
via CLI flags without touching any pipeline logic.
"""
