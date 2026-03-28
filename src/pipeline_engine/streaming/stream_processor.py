"""Stream processing abstractions: windowed aggregations, joins, and watermarks."""

from __future__ import annotations

import hashlib
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

logger = logging.getLogger(__name__)


class WindowType(str, Enum):
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"


@dataclass
class WindowConfig:
    """Configuration for a processing window.

    Parameters
    ----------
    type:
        Window type (tumbling, sliding, or session).
    size_seconds:
        Window size in seconds.
    slide_seconds:
        Slide interval for sliding windows (ignored for tumbling/session).
    gap_seconds:
        Inactivity gap for session windows (ignored for tumbling/sliding).
    """

    type: WindowType = WindowType.TUMBLING
    size_seconds: float = 60.0
    slide_seconds: float | None = None
    gap_seconds: float | None = None

    def __post_init__(self) -> None:
        if self.type == WindowType.SLIDING and not self.slide_seconds:
            self.slide_seconds = self.size_seconds / 2
        if self.type == WindowType.SESSION and not self.gap_seconds:
            self.gap_seconds = self.size_seconds


@dataclass
class WindowState:
    """State for a single window instance."""

    window_start: float
    window_end: float
    records: list[dict[str, Any]] = field(default_factory=list)

    @property
    def record_count(self) -> int:
        return len(self.records)

    def is_expired(self, current_time: float) -> bool:
        return current_time >= self.window_end


@dataclass
class WatermarkState:
    """Tracks event-time watermark for late event handling.

    Parameters
    ----------
    max_lateness_seconds:
        Maximum allowed lateness for events. Events arriving later
        than this behind the watermark are dropped.
    """

    max_lateness_seconds: float = 10.0
    current_watermark: float = 0.0
    late_events_dropped: int = 0

    def advance(self, event_time: float) -> None:
        """Advance the watermark based on an observed event time."""
        proposed = event_time - self.max_lateness_seconds
        if proposed > self.current_watermark:
            self.current_watermark = proposed

    def is_late(self, event_time: float) -> bool:
        """Check if an event is too late to be processed."""
        return event_time < self.current_watermark


class WindowedAggregator:
    """Performs windowed aggregations over streaming records.

    Supports tumbling, sliding, and session windows with configurable
    group-by keys and aggregation functions.

    Parameters
    ----------
    config:
        Window configuration.
    group_by:
        List of field names to group records by.
    aggregations:
        Dict mapping output field names to aggregation specs.
        Supported: ``sum(field)``, ``count(*)``, ``avg(field)``,
        ``min(field)``, ``max(field)``.
    timestamp_field:
        Field name containing event timestamps (epoch seconds).
        If ``None``, processing time is used.
    watermark:
        Optional watermark state for late event handling.

    Example::

        aggregator = WindowedAggregator(
            config=WindowConfig(type=WindowType.TUMBLING, size_seconds=60),
            group_by=["merchant_category"],
            aggregations={
                "total_amount": "sum(amount)",
                "tx_count": "count(*)",
            },
        )
        results = aggregator.process(records)
    """

    def __init__(
        self,
        config: WindowConfig,
        group_by: list[str] | None = None,
        aggregations: dict[str, str] | None = None,
        timestamp_field: str | None = None,
        watermark: WatermarkState | None = None,
    ) -> None:
        self._config = config
        self._group_by = group_by or []
        self._aggregations = aggregations or {}
        self._timestamp_field = timestamp_field
        self._watermark = watermark
        self._windows: dict[str, list[WindowState]] = defaultdict(list)
        self._parsed_aggs = self._parse_aggregations()

    @property
    def config(self) -> WindowConfig:
        return self._config

    def _parse_aggregations(self) -> list[tuple[str, str, str]]:
        """Parse aggregation specs into (output_name, function, field) tuples."""
        parsed: list[tuple[str, str, str]] = []
        for name, spec in self._aggregations.items():
            spec = spec.strip()
            if "(" not in spec:
                parsed.append((name, spec, ""))
                continue

            func = spec[: spec.index("(")].strip().lower()
            arg = spec[spec.index("(") + 1 : spec.index(")")].strip()
            parsed.append((name, func, arg))
        return parsed

    def _get_group_key(self, record: dict[str, Any]) -> str:
        """Compute a group key from the record's group-by fields."""
        if not self._group_by:
            return "__all__"
        values = tuple(record.get(k, None) for k in self._group_by)
        return hashlib.md5(str(values).encode()).hexdigest()

    def _get_event_time(self, record: dict[str, Any]) -> float:
        """Extract event time from a record, or use current time."""
        if self._timestamp_field and self._timestamp_field in record:
            ts = record[self._timestamp_field]
            return float(ts)
        return time.time()

    def _get_window_start(self, event_time: float) -> float:
        """Calculate window start time for a tumbling window."""
        size = self._config.size_seconds
        return (event_time // size) * size

    def process(
        self, records: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Process a batch of records through the windowed aggregation.

        Returns aggregated results for any windows that have closed.
        """
        results: list[dict[str, Any]] = []

        for record in records:
            event_time = self._get_event_time(record)

            # Check watermark for late events
            if self._watermark:
                self._watermark.advance(event_time)
                if self._watermark.is_late(event_time):
                    self._watermark.late_events_dropped += 1
                    continue

            group_key = self._get_group_key(record)

            if self._config.type == WindowType.TUMBLING:
                self._add_to_tumbling_window(group_key, record, event_time)
            elif self._config.type == WindowType.SLIDING:
                self._add_to_sliding_windows(group_key, record, event_time)
            elif self._config.type == WindowType.SESSION:
                self._add_to_session_window(group_key, record, event_time)

        # Emit results from closed windows
        current_time = time.time()
        for group_key, windows in list(self._windows.items()):
            expired = [w for w in windows if w.is_expired(current_time)]
            for window in expired:
                agg = self._aggregate_window(window, group_key)
                results.append(agg)
                windows.remove(window)

        return results

    def flush(self) -> list[dict[str, Any]]:
        """Force-emit results from all open windows."""
        results: list[dict[str, Any]] = []
        for group_key, windows in self._windows.items():
            for window in windows:
                if window.records:
                    agg = self._aggregate_window(window, group_key)
                    results.append(agg)
        self._windows.clear()
        return results

    def _add_to_tumbling_window(
        self, group_key: str, record: dict[str, Any], event_time: float
    ) -> None:
        """Add a record to the appropriate tumbling window."""
        window_start = self._get_window_start(event_time)
        window_end = window_start + self._config.size_seconds

        windows = self._windows[group_key]
        target = None
        for w in windows:
            if w.window_start == window_start:
                target = w
                break

        if target is None:
            target = WindowState(window_start=window_start, window_end=window_end)
            windows.append(target)

        target.records.append(record)

    def _add_to_sliding_windows(
        self, group_key: str, record: dict[str, Any], event_time: float
    ) -> None:
        """Add a record to all applicable sliding windows."""
        slide = self._config.slide_seconds or self._config.size_seconds / 2
        size = self._config.size_seconds

        # Find the earliest window start that contains this event
        earliest_start = event_time - size + slide
        earliest_start = (earliest_start // slide) * slide

        windows = self._windows[group_key]
        current_start = earliest_start
        while current_start <= event_time:
            window_end = current_start + size
            target = None
            for w in windows:
                if abs(w.window_start - current_start) < 0.001:
                    target = w
                    break

            if target is None:
                target = WindowState(
                    window_start=current_start, window_end=window_end
                )
                windows.append(target)

            target.records.append(record)
            current_start += slide

    def _add_to_session_window(
        self, group_key: str, record: dict[str, Any], event_time: float
    ) -> None:
        """Add a record to a session window, creating or merging as needed."""
        gap = self._config.gap_seconds or self._config.size_seconds
        windows = self._windows[group_key]

        # Find an existing session this event belongs to
        for w in windows:
            if event_time <= w.window_end + gap:
                w.records.append(record)
                w.window_end = max(w.window_end, event_time)
                return

        # Create new session
        windows.append(
            WindowState(
                window_start=event_time,
                window_end=event_time + gap,
                records=[record],
            )
        )

    def _aggregate_window(
        self, window: WindowState, group_key: str
    ) -> dict[str, Any]:
        """Compute aggregations for a completed window."""
        result: dict[str, Any] = {
            "_window_start": window.window_start,
            "_window_end": window.window_end,
            "_record_count": window.record_count,
        }

        # Add group-by values from first record
        if window.records and self._group_by:
            for key in self._group_by:
                result[key] = window.records[0].get(key)

        # Compute aggregations
        for name, func, field_name in self._parsed_aggs:
            if func == "count":
                result[name] = len(window.records)
            elif func == "sum":
                result[name] = sum(
                    r.get(field_name, 0) for r in window.records
                )
            elif func == "avg":
                values = [r.get(field_name, 0) for r in window.records]
                result[name] = sum(values) / len(values) if values else 0
            elif func == "min":
                values = [r.get(field_name) for r in window.records if field_name in r]
                result[name] = min(values) if values else None
            elif func == "max":
                values = [r.get(field_name) for r in window.records if field_name in r]
                result[name] = max(values) if values else None

        return result


class StreamJoin:
    """Key-based stream join for combining two record streams.

    Maintains a buffer of records from each side and joins them on matching
    keys when both sides have been seen.

    Parameters
    ----------
    left_key:
        Field name to join on from the left stream.
    right_key:
        Field name to join on from the right stream.
    window_seconds:
        Time window in seconds for matching records from both sides.

    Example::

        join = StreamJoin(left_key="order_id", right_key="order_id")
        join.add_left([{"order_id": "A", "amount": 100}])
        join.add_right([{"order_id": "A", "status": "shipped"}])
        results = join.emit()
        # [{"order_id": "A", "amount": 100, "status": "shipped"}]
    """

    def __init__(
        self,
        left_key: str,
        right_key: str,
        window_seconds: float = 300.0,
    ) -> None:
        self._left_key = left_key
        self._right_key = right_key
        self._window_seconds = window_seconds
        self._left_buffer: dict[str, list[tuple[float, dict[str, Any]]]] = defaultdict(list)
        self._right_buffer: dict[str, list[tuple[float, dict[str, Any]]]] = defaultdict(list)

    def add_left(self, records: list[dict[str, Any]]) -> None:
        """Add records from the left stream."""
        now = time.time()
        for record in records:
            key = str(record.get(self._left_key, ""))
            self._left_buffer[key].append((now, record))

    def add_right(self, records: list[dict[str, Any]]) -> None:
        """Add records from the right stream."""
        now = time.time()
        for record in records:
            key = str(record.get(self._right_key, ""))
            self._right_buffer[key].append((now, record))

    def emit(self) -> list[dict[str, Any]]:
        """Emit joined records where both sides have matching keys."""
        self._evict_expired()
        results: list[dict[str, Any]] = []

        for key in set(self._left_buffer) & set(self._right_buffer):
            for _, left_rec in self._left_buffer[key]:
                for _, right_rec in self._right_buffer[key]:
                    merged = {**left_rec, **right_rec}
                    results.append(merged)

            # Clear matched entries
            del self._left_buffer[key]
            del self._right_buffer[key]

        return results

    def _evict_expired(self) -> None:
        """Remove records older than the join window."""
        cutoff = time.time() - self._window_seconds

        for key in list(self._left_buffer):
            self._left_buffer[key] = [
                (t, r) for t, r in self._left_buffer[key] if t >= cutoff
            ]
            if not self._left_buffer[key]:
                del self._left_buffer[key]

        for key in list(self._right_buffer):
            self._right_buffer[key] = [
                (t, r) for t, r in self._right_buffer[key] if t >= cutoff
            ]
            if not self._right_buffer[key]:
                del self._right_buffer[key]

    @property
    def left_buffer_size(self) -> int:
        return sum(len(v) for v in self._left_buffer.values())

    @property
    def right_buffer_size(self) -> int:
        return sum(len(v) for v in self._right_buffer.values())

    def __repr__(self) -> str:
        return (
            f"StreamJoin(left_key={self._left_key!r}, "
            f"right_key={self._right_key!r}, "
            f"window={self._window_seconds}s)"
        )


class IdempotentWriter:
    """Ensures exactly-once semantics via idempotent write tracking.

    Tracks record IDs that have been successfully written and skips
    duplicates. Uses a bounded LRU-style cache to limit memory.

    Parameters
    ----------
    id_field:
        Field name used to extract the unique record identifier.
    max_tracked:
        Maximum number of IDs to track before evicting oldest entries.
    """

    def __init__(self, id_field: str = "id", max_tracked: int = 100_000) -> None:
        self._id_field = id_field
        self._max_tracked = max_tracked
        self._seen: dict[str, float] = {}
        self._order: list[str] = []

    def filter_duplicates(
        self, records: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Filter out previously seen records.

        Returns only records whose ID has not been seen before.
        """
        unique: list[dict[str, Any]] = []
        for record in records:
            record_id = str(record.get(self._id_field, ""))
            if not record_id or record_id in self._seen:
                continue

            self._mark_seen(record_id)
            unique.append(record)

        return unique

    def _mark_seen(self, record_id: str) -> None:
        """Mark a record ID as seen."""
        self._seen[record_id] = time.time()
        self._order.append(record_id)

        # Evict oldest entries if over capacity
        while len(self._order) > self._max_tracked:
            old_id = self._order.pop(0)
            self._seen.pop(old_id, None)

    @property
    def tracked_count(self) -> int:
        return len(self._seen)

    def __repr__(self) -> str:
        return (
            f"IdempotentWriter(id_field={self._id_field!r}, "
            f"tracked={self.tracked_count})"
        )
