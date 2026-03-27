"""Task scheduler with dependency-aware batch dispatch."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pipeline_engine.core.state import RunStatus

if TYPE_CHECKING:
    from pipeline_engine.core.dag import DAG
    from pipeline_engine.core.state import PipelineState

logger = logging.getLogger(__name__)


class Scheduler:
    """Dependency-resolving scheduler that emits execution batches.

    The scheduler does not execute anything itself — it determines *which* nodes
    are eligible to run given the current pipeline state and parallelism cap.

    Args:
        dag: The pipeline graph.
        state: Mutable pipeline state tracking node statuses.
        max_parallel: Maximum number of nodes to dispatch in a single batch.
    """

    def __init__(self, dag: DAG, state: PipelineState, max_parallel: int = 4) -> None:
        if max_parallel < 1:
            raise ValueError(f"max_parallel must be >= 1, got {max_parallel}")
        self.dag = dag
        self.state = state
        self.max_parallel = max_parallel

    def get_next_batch(self) -> list[str]:
        """Return up to *max_parallel* node IDs that are ready for execution.

        A node is ready when:
        - Its status is PENDING.
        - All of its direct dependencies have status COMPLETED.
        - No node in the pipeline has FAILED (we stop scheduling on failure).

        Returns an empty list when nothing can be dispatched (either because
        everything is done, something failed, or in-flight nodes must finish
        before new ones become eligible).
        """
        if self.has_failed():
            logger.debug("Scheduler halted: pipeline has failed nodes")
            return []

        ready = self.state.get_ready_nodes(self.dag)

        # Respect the parallelism cap, factoring in already-running nodes.
        running_count = sum(
            1
            for ns in self.state.node_states.values()
            if ns.status == RunStatus.RUNNING
        )
        available_slots = max(0, self.max_parallel - running_count)
        batch = ready[:available_slots]

        if batch:
            logger.debug(
                "Scheduler dispatching batch: %s (running=%d, available_slots=%d)",
                batch,
                running_count,
                available_slots,
            )
        return batch

    def is_complete(self) -> bool:
        """True when execution should stop: all nodes finished or any node failed."""
        if self.has_failed():
            return True
        return all(
            ns.status == RunStatus.COMPLETED
            for ns in self.state.node_states.values()
        )

    def has_failed(self) -> bool:
        """True if any node in the pipeline has FAILED status."""
        return any(
            ns.status == RunStatus.FAILED
            for ns in self.state.node_states.values()
        )

    def __repr__(self) -> str:
        return (
            f"Scheduler(dag={self.dag.name!r}, max_parallel={self.max_parallel}, "
            f"complete={self.is_complete()})"
        )
