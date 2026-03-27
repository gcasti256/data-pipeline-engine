"""Async pipeline execution engine with retry, timeout, and parallel dispatch."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from pipeline_engine.core.dag import DAG
from pipeline_engine.core.scheduler import Scheduler
from pipeline_engine.core.state import NodeState, PipelineState

logger = logging.getLogger(__name__)

# Retry backoff constants.
_BASE_BACKOFF_SECONDS = 1.0
_BACKOFF_MULTIPLIER = 2.0
_MAX_BACKOFF_SECONDS = 60.0


class PipelineExecutor:
    """Executes a DAG-based pipeline with async parallelism and retry logic.

    The executor owns the full lifecycle of a single pipeline run:
    1. Validate the DAG and initialize state.
    2. Loop: ask the Scheduler for the next batch, execute nodes concurrently.
    3. Retry transient failures with exponential backoff (per-node config).
    4. Expose results so downstream nodes can consume upstream outputs.

    Args:
        dag: The pipeline graph to execute.
        max_parallel: Maximum concurrent node executions.
        on_node_complete: Optional callback invoked after each node finishes
            (success or failure). Receives ``(node_id, node_state)``.

    Example::

        dag = build_my_dag()
        executor = PipelineExecutor(dag, max_parallel=8)
        state = await executor.execute()
        print(state.to_dict())
    """

    def __init__(
        self,
        dag: DAG,
        max_parallel: int = 4,
        on_node_complete: Callable[[str, NodeState], Any] | None = None,
    ) -> None:
        if max_parallel < 1:
            raise ValueError(f"max_parallel must be >= 1, got {max_parallel}")
        self.dag = dag
        self.max_parallel = max_parallel
        self.on_node_complete = on_node_complete

        # Results dict: node_id -> output value. Downstream nodes can read
        # their dependencies' outputs through this mapping.
        self._results: dict[str, Any] = {}

    @property
    def results(self) -> dict[str, Any]:
        """Read-only view of node outputs keyed by node id."""
        return dict(self._results)


    async def execute(self) -> PipelineState:
        """Run the pipeline to completion and return the final state.

        The DAG is validated before execution begins. If validation raises,
        the exception propagates without creating a run.

        Returns:
            PipelineState with terminal status (COMPLETED or FAILED).
        """
        self.dag.validate()

        state = PipelineState(pipeline_name=self.dag.name)
        state.initialize(self.dag)
        self._results.clear()

        scheduler = Scheduler(
            dag=self.dag,
            state=state,
            max_parallel=self.max_parallel,
        )

        logger.info(
            "Starting pipeline '%s' (run_id=%s, nodes=%d, max_parallel=%d)",
            self.dag.name,
            state.run_id,
            len(self.dag),
            self.max_parallel,
        )

        while not scheduler.is_complete():
            batch = scheduler.get_next_batch()
            if not batch:
                # Nodes are still running but nothing new to dispatch — wait
                # briefly to avoid a hot spin loop.
                await asyncio.sleep(0.05)
                continue

            logger.info("Dispatching batch: %s", batch)
            tasks = [
                asyncio.create_task(
                    self._execute_node_with_retries(node_id, state),
                    name=f"node-{node_id}",
                )
                for node_id in batch
            ]
            # Wait for this batch to settle before scheduling the next round.
            # Using return_exceptions so a single failure doesn't cancel siblings.
            await asyncio.gather(*tasks, return_exceptions=True)

        # Log final status.
        duration = state.duration or 0.0
        logger.info(
            "Pipeline '%s' finished: status=%s, duration=%.2fs, run_id=%s",
            self.dag.name,
            state.status.value,
            duration,
            state.run_id,
        )

        return state


    async def _execute_node_with_retries(
        self,
        node_id: str,
        state: PipelineState,
    ) -> None:
        """Execute a node with retry and exponential backoff on failure."""
        node = self.dag.nodes[node_id]
        max_attempts = node.retries + 1  # first attempt + retries
        last_error: Exception | None = None

        state.mark_running(node_id)
        ns = state.node_states[node_id]

        for attempt in range(1, max_attempts + 1):
            try:
                logger.debug(
                    "Executing node '%s' (attempt %d/%d)",
                    node_id,
                    attempt,
                    max_attempts,
                )
                result = await self._execute_node(node_id, state)
                self._results[node_id] = result

                # Determine output record count heuristically.
                output_records = self._count_records(result)
                state.mark_completed(node_id, output_records=output_records)
                logger.info(
                    "Node '%s' completed (output_records=%d, attempt=%d)",
                    node_id,
                    output_records,
                    attempt,
                )
                self._fire_callback(node_id, ns)
                return

            except Exception as exc:
                last_error = exc
                ns.retries_used = attempt  # how many attempts consumed so far
                logger.warning(
                    "Node '%s' failed on attempt %d/%d: %s",
                    node_id,
                    attempt,
                    max_attempts,
                    exc,
                )

                if attempt < max_attempts:
                    backoff = min(
                        _BASE_BACKOFF_SECONDS * (_BACKOFF_MULTIPLIER ** (attempt - 1)),
                        _MAX_BACKOFF_SECONDS,
                    )
                    logger.debug("Retrying node '%s' after %.1fs backoff", node_id, backoff)
                    await asyncio.sleep(backoff)

        # All attempts exhausted.
        error_msg = f"{type(last_error).__name__}: {last_error}" if last_error else "Unknown error"
        state.mark_failed(node_id, error=error_msg)
        logger.error(
            "Node '%s' permanently failed after %d attempts: %s",
            node_id, max_attempts, error_msg,
        )
        self._fire_callback(node_id, ns)

    async def _execute_node(self, node_id: str, state: PipelineState) -> Any:
        """Execute a single node's operation, handling both sync and async callables.

        The operation receives a context dict containing:
        - ``config``: The node's static configuration.
        - ``node_id``: The node's identifier.
        - ``results``: Outputs from upstream dependencies (read-only copy).
        - ``dependencies``: Set of dependency node ids.

        If the node specifies a timeout, the call is wrapped in ``asyncio.wait_for``.
        """
        node = self.dag.nodes[node_id]
        deps = self.dag.get_dependencies(node_id)

        # Build the context passed to the operation.
        context: dict[str, Any] = {
            "config": dict(node.config),
            "node_id": node_id,
            "results": {dep: self._results.get(dep) for dep in deps},
            "dependencies": deps,
        }

        # Track input records from upstream.
        input_records = sum(
            state.node_states[dep].output_records for dep in deps
        )
        state.node_states[node_id].input_records = input_records

        # Invoke the operation.
        coro = self._invoke_operation(node.operation, context)

        if node.timeout is not None:
            return await asyncio.wait_for(coro, timeout=node.timeout)
        return await coro

    @staticmethod
    async def _invoke_operation(operation: Callable[..., Any], context: dict[str, Any]) -> Any:
        """Call the operation, handling both sync and async functions."""
        result = operation(context)
        if asyncio.iscoroutine(result):
            return await result
        return result


    @staticmethod
    def _count_records(result: Any) -> int:
        """Best-effort record count from a node's output.

        Returns ``len(result)`` for sized iterables, 1 for non-None scalars, 0 for None.
        """
        if result is None:
            return 0
        if hasattr(result, "__len__"):
            try:
                return len(result)
            except TypeError:
                pass
        return 1

    def _fire_callback(self, node_id: str, node_state: NodeState) -> None:
        """Invoke the on_node_complete callback if configured, swallowing errors."""
        if self.on_node_complete is None:
            return
        try:
            self.on_node_complete(node_id, node_state)
        except Exception:
            logger.exception("on_node_complete callback raised for node '%s'", node_id)
