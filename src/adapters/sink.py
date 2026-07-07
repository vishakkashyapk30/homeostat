"""Abstract sink interface for clean + dead-letter (quarantine) records."""

from abc import ABC, abstractmethod


class Sink(ABC):
    @abstractmethod
    def write_clean(self, cycle_id: int, records: list[dict]) -> str:
        """Persist successfully transformed records. Returns a location string."""

    @abstractmethod
    def write_quarantine(self, cycle_id: int, records_with_reasons: list[dict]) -> str:
        """Persist rejected records with reason codes. Returns a location string."""

    def name(self) -> str:
        return self.__class__.__name__
