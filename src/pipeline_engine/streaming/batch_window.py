"""Time- and count-based batch windowing for streaming pipelines."""

from __future__ import annotations

import threading
import time
from typing import Any


class BatchWindow:
    """Accumulates records and flushes them as batches.

    A batch is emitted when **either** of two conditions is met:

    1. The number of buffered records reaches *max_size*.
    2. The elapsed time since the first record in the current batch exceeds
       *max_wait_seconds*.

    This class is thread-safe; multiple producers may call :meth:`add`
    concurrently.

    Parameters
    ----------
    max_size:
        Maximum number of records per batch.
    max_wait_seconds:
        Maximum wall-clock seconds to wait before flushing a partial batch.

    Example::

        window = BatchWindow(max_size=100, max_wait_seconds=5.0)
        for record in stream:
            batch = window.add(record)
            if batch is not None:
                process(batch)
        # Don't forget the trailing partial batch.
        remaining = window.flush()
        if remaining:
            process(remaining)
    """

    def __init__(
        self,
        max_size: int = 100,
        max_wait_seconds: float = 10.0,
    ) -> None:
        if max_size < 1:
            raise ValueError(f"max_size must be >= 1, got {max_size}")
        if max_wait_seconds <= 0:
            raise ValueError(
                f"max_wait_seconds must be positive, got {max_wait_seconds}"
            )
        self._max_size = max_size
        self._max_wait_seconds = max_wait_seconds
        self._buffer: list[dict[str, Any]] = []
        self._batch_start: float | None = None
        self._lock = threading.Lock()


    def add(self, record: dict[str, Any]) -> list[dict[str, Any]] | None:
        """Add a record to the current batch.

        Returns the completed batch when the window closes (by size or
        time), otherwise returns ``None``.

        Args:
            record: A single record dict.

        Returns:
            A list of records if the batch is complete, or ``None``.
        """
        with self._lock:
            if not self._buffer:
                self._batch_start = time.monotonic()

            self._buffer.append(record)

            if len(self._buffer) >= self._max_size:
                return self._flush_locked()

            if self._check_time_window():
                return self._flush_locked()

        return None

    def flush(self) -> list[dict[str, Any]]:
        """Force-flush the current batch regardless of size or time.

        Returns:
            The buffered records (may be empty).
        """
        with self._lock:
            return self._flush_locked()

    @property
    def pending_count(self) -> int:
        """Number of records currently buffered."""
        with self._lock:
            return len(self._buffer)

    @property
    def max_size(self) -> int:
        """Configured maximum batch size."""
        return self._max_size

    @property
    def max_wait_seconds(self) -> float:
        """Configured maximum wait time in seconds."""
        return self._max_wait_seconds


    def _check_time_window(self) -> bool:
        """Return ``True`` if the time window has been exceeded.

        Must be called while holding ``self._lock``.
        """
        if self._batch_start is None:
            return False
        elapsed = time.monotonic() - self._batch_start
        return elapsed >= self._max_wait_seconds

    def _flush_locked(self) -> list[dict[str, Any]]:
        """Return the current buffer and reset state.

        Must be called while holding ``self._lock``.
        """
        batch = list(self._buffer)
        self._buffer.clear()
        self._batch_start = None
        return batch

    def __repr__(self) -> str:
        return (
            f"BatchWindow(max_size={self._max_size}, "
            f"max_wait_seconds={self._max_wait_seconds}, "
            f"pending={self.pending_count})"
        )
