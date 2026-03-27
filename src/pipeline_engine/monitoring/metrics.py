"""Metrics collection for pipeline execution monitoring."""

from __future__ import annotations

import threading
from typing import Any

from pipeline_engine.core.state import PipelineState, RunStatus


class MetricsCollector:
    """Centralized store for pipeline run metrics.

    Records completed pipeline runs and provides aggregate statistics.
    Thread-safe for concurrent access from API routes and pipeline
    executors.

    Typical usage::

        collector = get_collector()
        collector.record_run(state)

        # Later, from the monitoring API:
        runs = collector.get_runs()
        aggregates = collector.get_aggregate_metrics()
    """

    _instance: MetricsCollector | None = None
    _init_lock = threading.Lock()

    def __init__(self) -> None:
        self._runs: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()


    @classmethod
    def get_instance(cls) -> MetricsCollector:
        """Return the module-level singleton instance.

        Creates one on first call; subsequent calls return the same object.
        """
        if cls._instance is None:
            with cls._init_lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """Discard the singleton (useful in tests)."""
        with cls._init_lock:
            cls._instance = None


    def record_run(self, state: PipelineState) -> None:
        """Store a completed pipeline run.

        Args:
            state: The :class:`PipelineState` at the end of execution.
        """
        run_data = state.to_dict()
        with self._lock:
            self._runs[state.run_id] = run_data


    def get_runs(self) -> list[dict[str, Any]]:
        """Return all recorded runs, newest first.

        Returns:
            List of run dicts (each produced by ``PipelineState.to_dict``).
        """
        with self._lock:
            runs = list(self._runs.values())
        # Sort by created_at descending.
        runs.sort(key=lambda r: r.get("created_at", ""), reverse=True)
        return runs

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        """Return details for a specific run.

        Args:
            run_id: The pipeline run identifier.

        Returns:
            The run dict, or ``None`` if not found.
        """
        with self._lock:
            return self._runs.get(run_id)

    def get_aggregate_metrics(self) -> dict[str, Any]:
        """Compute aggregate statistics across all recorded runs.

        Returns a dict with the following keys:

        - ``total_runs``: Number of recorded runs.
        - ``successful_runs``: Number of runs that completed successfully.
        - ``failed_runs``: Number of runs that failed.
        - ``success_rate``: Fraction of runs that succeeded (0.0 -- 1.0).
        - ``error_rate``: Fraction of runs that failed (0.0 -- 1.0).
        - ``avg_duration_seconds``: Average duration of completed runs.
        - ``total_records_processed``: Sum of output records across all nodes
          in all runs.
        """
        with self._lock:
            runs = list(self._runs.values())

        total = len(runs)
        if total == 0:
            return {
                "total_runs": 0,
                "successful_runs": 0,
                "failed_runs": 0,
                "success_rate": 0.0,
                "error_rate": 0.0,
                "avg_duration_seconds": 0.0,
                "total_records_processed": 0,
            }

        successful = sum(
            1 for r in runs if r.get("status") == RunStatus.COMPLETED.value
        )
        failed = sum(
            1 for r in runs if r.get("status") == RunStatus.FAILED.value
        )

        durations: list[float] = [
            r.get("duration_seconds", 0.0)
            for r in runs
            if r.get("duration_seconds") is not None
        ]
        avg_duration = (
            sum(durations) / len(durations) if durations else 0.0
        )

        total_records = 0
        for run in runs:
            nodes = run.get("nodes", {})
            for node_data in nodes.values():
                total_records += node_data.get("output_records", 0)

        return {
            "total_runs": total,
            "successful_runs": successful,
            "failed_runs": failed,
            "success_rate": successful / total,
            "error_rate": failed / total,
            "avg_duration_seconds": round(avg_duration, 3),
            "total_records_processed": total_records,
        }


def get_collector() -> MetricsCollector:
    """Module-level convenience accessor for the singleton collector."""
    return MetricsCollector.get_instance()
