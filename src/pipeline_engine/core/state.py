"""Pipeline and node state tracking for execution monitoring and recovery."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pipeline_engine.core.dag import DAG


class RunStatus(Enum):
    """Lifecycle status of a pipeline run or individual node execution."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class NodeState:
    """Mutable execution state for a single pipeline node.

    Attributes:
        node_id: Identifier matching the corresponding DAG node.
        status: Current lifecycle status.
        started_at: Timestamp when execution began.
        completed_at: Timestamp when execution finished (success or failure).
        error: Error message if the node failed.
        input_records: Number of records received as input.
        output_records: Number of records produced as output.
        retries_used: How many retry attempts were consumed.
    """

    node_id: str
    status: RunStatus = RunStatus.PENDING
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None
    input_records: int = 0
    output_records: int = 0
    retries_used: int = 0

    @property
    def duration_seconds(self) -> float | None:
        """Wall-clock duration of this node's execution, or None if not yet started."""
        if self.started_at is None:
            return None
        end = self.completed_at or datetime.now(UTC)
        return (end - self.started_at).total_seconds()

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a plain dict suitable for JSON encoding or storage."""
        return {
            "node_id": self.node_id,
            "status": self.status.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error": self.error,
            "input_records": self.input_records,
            "output_records": self.output_records,
            "retries_used": self.retries_used,
            "duration_seconds": self.duration_seconds,
        }


class PipelineState:
    """Aggregate execution state for an entire pipeline run.

    Tracks the status of every node in the DAG plus overall run metadata.
    Designed for monitoring, persistence, and recovery workflows.

    Attributes:
        run_id: Unique identifier for this execution run.
        pipeline_name: Name of the DAG being executed.
        status: Aggregate run status.
        created_at: Timestamp when the run was created.
        node_states: Per-node execution state keyed by node id.
    """

    def __init__(self, pipeline_name: str, run_id: str | None = None) -> None:
        self.run_id: str = run_id or uuid.uuid4().hex
        self.pipeline_name: str = pipeline_name
        self.status: RunStatus = RunStatus.PENDING
        self.created_at: datetime = datetime.now(UTC)
        self.started_at: datetime | None = None
        self.completed_at: datetime | None = None
        self.node_states: dict[str, NodeState] = {}


    def initialize(self, dag: DAG) -> None:
        """Create a PENDING NodeState for every node in the DAG.

        This must be called before execution begins. Calling it again replaces
        all existing node states.
        """
        self.node_states = {
            node_id: NodeState(node_id=node_id) for node_id in dag.nodes
        }
        self.status = RunStatus.PENDING


    def mark_running(self, node_id: str) -> None:
        """Transition a node to RUNNING status.

        Raises:
            KeyError: If *node_id* is not tracked.
            ValueError: If the node is not in PENDING status.
        """
        ns = self._get_node_state(node_id)
        if ns.status != RunStatus.PENDING:
            raise ValueError(
                f"Cannot mark node '{node_id}' as running: current status is {ns.status.value}"
            )
        ns.status = RunStatus.RUNNING
        ns.started_at = datetime.now(UTC)

        # Promote the overall pipeline to RUNNING on the first node start.
        if self.status == RunStatus.PENDING:
            self.status = RunStatus.RUNNING
            self.started_at = datetime.now(UTC)

    def mark_completed(self, node_id: str, output_records: int = 0) -> None:
        """Transition a node to COMPLETED status.

        Args:
            node_id: The node to mark.
            output_records: Number of output records produced by this node.

        Raises:
            KeyError: If *node_id* is not tracked.
            ValueError: If the node is not in RUNNING status.
        """
        ns = self._get_node_state(node_id)
        if ns.status != RunStatus.RUNNING:
            raise ValueError(
                f"Cannot mark node '{node_id}' as completed: current status is {ns.status.value}"
            )
        ns.status = RunStatus.COMPLETED
        ns.completed_at = datetime.now(UTC)
        ns.output_records = output_records

        # Check if the entire pipeline is done.
        self._maybe_finalize()

    def mark_failed(self, node_id: str, error: str) -> None:
        """Transition a node to FAILED status.

        Args:
            node_id: The node to mark.
            error: Human-readable error description.

        Raises:
            KeyError: If *node_id* is not tracked.
            ValueError: If the node is not in RUNNING status.
        """
        ns = self._get_node_state(node_id)
        if ns.status != RunStatus.RUNNING:
            raise ValueError(
                f"Cannot mark node '{node_id}' as failed: current status is {ns.status.value}"
            )
        ns.status = RunStatus.FAILED
        ns.completed_at = datetime.now(UTC)
        ns.error = error

        # A single node failure fails the entire run.
        self.status = RunStatus.FAILED
        self.completed_at = datetime.now(UTC)

    def cancel(self) -> None:
        """Cancel the entire pipeline run.

        Pending nodes are moved to CANCELLED; running nodes are left as-is
        (the caller is responsible for interrupting in-flight work).
        """
        for ns in self.node_states.values():
            if ns.status == RunStatus.PENDING:
                ns.status = RunStatus.CANCELLED
        self.status = RunStatus.CANCELLED
        self.completed_at = datetime.now(UTC)


    def get_ready_nodes(self, dag: DAG) -> list[str]:
        """Return node ids whose dependencies are all COMPLETED and which are still PENDING.

        This is the primary interface used by the Scheduler to determine what can
        be dispatched next.
        """
        ready: list[str] = []
        for node_id, ns in self.node_states.items():
            if ns.status != RunStatus.PENDING:
                continue
            deps = dag.get_dependencies(node_id)
            if all(
                self.node_states[dep].status == RunStatus.COMPLETED for dep in deps
            ):
                ready.append(node_id)
        return sorted(ready)  # sorted for deterministic scheduling

    @property
    def duration(self) -> float | None:
        """Total elapsed wall-clock time in seconds, or None if not yet started."""
        if self.started_at is None:
            return None
        end = self.completed_at or datetime.now(UTC)
        return (end - self.started_at).total_seconds()

    @property
    def is_terminal(self) -> bool:
        """True if the run has reached a final state (completed, failed, or cancelled)."""
        return self.status in (RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED)


    def to_dict(self) -> dict[str, Any]:
        """Serialize the full pipeline state to a plain dict."""
        return {
            "run_id": self.run_id,
            "pipeline_name": self.pipeline_name,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": self.duration,
            "nodes": {nid: ns.to_dict() for nid, ns in self.node_states.items()},
            "summary": {
                "total": len(self.node_states),
                "pending": self._count_status(RunStatus.PENDING),
                "running": self._count_status(RunStatus.RUNNING),
                "completed": self._count_status(RunStatus.COMPLETED),
                "failed": self._count_status(RunStatus.FAILED),
                "cancelled": self._count_status(RunStatus.CANCELLED),
            },
        }


    def _get_node_state(self, node_id: str) -> NodeState:
        """Retrieve a NodeState or raise KeyError with a clear message."""
        try:
            return self.node_states[node_id]
        except KeyError:
            raise KeyError(f"No state tracked for node '{node_id}'") from None

    def _count_status(self, status: RunStatus) -> int:
        return sum(1 for ns in self.node_states.values() if ns.status == status)

    def _maybe_finalize(self) -> None:
        """Promote the pipeline to COMPLETED if every node has finished successfully."""
        if all(ns.status == RunStatus.COMPLETED for ns in self.node_states.values()):
            self.status = RunStatus.COMPLETED
            self.completed_at = datetime.now(UTC)

    def __repr__(self) -> str:
        return (
            f"PipelineState(run_id={self.run_id!r}, pipeline={self.pipeline_name!r}, "
            f"status={self.status.value}, nodes={len(self.node_states)})"
        )
