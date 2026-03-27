"""Dead letter queue for records that fail validation or processing."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class DeadLetterEntry:
    """A single failed record stored in the dead letter queue.

    Attributes:
        record: The original record dict that failed.
        error: Human-readable description of the failure.
        source: Optional identifier for the pipeline stage that produced
            the failure (e.g. ``"schema_validation"`` or ``"transform_map"``).
        timestamp: UTC timestamp when the entry was created.
    """

    record: dict[str, Any]
    error: str
    source: str = ""
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))


class DeadLetterQueue:
    """Thread-safe queue for records that fail validation or processing.

    Failed records are stored with metadata (error message, source stage,
    timestamp) so they can be inspected, retried, or exported later.

    Parameters
    ----------
    max_size:
        Maximum number of entries the queue will hold.  Once full, the
        oldest entry is evicted to make room for new ones.

    Example::

        dlq = DeadLetterQueue(max_size=5000)
        dlq.add({"id": 1, "value": -1}, error="Value out of range", source="validation")
        print(dlq.size)       # 1
        entries = dlq.flush()  # returns all entries and clears queue
    """

    def __init__(self, max_size: int = 10_000) -> None:
        if max_size < 1:
            raise ValueError(f"max_size must be >= 1, got {max_size}")
        self._max_size = max_size
        self._entries: list[DeadLetterEntry] = []
        self._lock = threading.Lock()


    def add(
        self,
        record: dict[str, Any],
        error: str,
        source: str = "",
    ) -> None:
        """Store a failed record in the queue.

        If the queue is at capacity, the oldest entry is discarded before
        the new one is appended.

        Args:
            record: The record that failed.
            error: Description of the failure.
            source: Optional identifier for the originating stage.
        """
        entry = DeadLetterEntry(
            record=dict(record),
            error=error,
            source=source,
        )
        with self._lock:
            if len(self._entries) >= self._max_size:
                self._entries.pop(0)
            self._entries.append(entry)

    def flush(self) -> list[DeadLetterEntry]:
        """Return all entries and clear the queue.

        Returns:
            A list of all :class:`DeadLetterEntry` objects that were queued.
        """
        with self._lock:
            entries = list(self._entries)
            self._entries.clear()
        return entries


    def get_all(self) -> list[DeadLetterEntry]:
        """Return a snapshot of all entries without clearing."""
        with self._lock:
            return list(self._entries)

    def get_by_source(self, source: str) -> list[DeadLetterEntry]:
        """Return entries originating from a specific source stage.

        Args:
            source: The source identifier to filter on.

        Returns:
            List of matching :class:`DeadLetterEntry` objects.
        """
        with self._lock:
            return [e for e in self._entries if e.source == source]

    @property
    def size(self) -> int:
        """Current number of entries in the queue."""
        with self._lock:
            return len(self._entries)

    @property
    def max_size(self) -> int:
        """Maximum capacity of the queue."""
        return self._max_size


    def to_records(self) -> list[dict[str, Any]]:
        """Serialize all entries to plain dicts suitable for export.

        Each entry is converted to a dict with keys ``record``, ``error``,
        ``source``, and ``timestamp`` (as an ISO 8601 string).

        Returns:
            List of serialized entry dicts.
        """
        with self._lock:
            return [
                {
                    "record": entry.record,
                    "error": entry.error,
                    "source": entry.source,
                    "timestamp": entry.timestamp.isoformat(),
                }
                for entry in self._entries
            ]


    def __len__(self) -> int:
        return self.size

    def __repr__(self) -> str:
        return (
            f"DeadLetterQueue(size={self.size}, max_size={self._max_size})"
        )
