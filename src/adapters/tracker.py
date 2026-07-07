"""Abstract tracker interface for run + incident observability."""

from abc import ABC, abstractmethod


class Tracker(ABC):
    @abstractmethod
    def log_run(self, entry: dict) -> None:
        """Record one pipeline run (always also persisted to the manifest)."""

    @abstractmethod
    def log_incident(self, incident: dict) -> None:
        """Record one self-healing incident resolution."""

    def name(self) -> str:
        return self.__class__.__name__
