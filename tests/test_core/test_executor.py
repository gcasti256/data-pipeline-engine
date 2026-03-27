"""Tests for pipeline_engine.core.executor — async pipeline execution with retry."""

from __future__ import annotations

import asyncio

import pytest

from pipeline_engine.core.dag import DAG, Node
from pipeline_engine.core.executor import PipelineExecutor
from pipeline_engine.core.state import RunStatus

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_sync_op(return_value):
    """Create a sync operation that returns a fixed value."""
    def _op(ctx):
        return return_value
    return _op


def _make_async_op(return_value, delay: float = 0):
    """Create an async operation that returns a fixed value after an optional delay."""
    async def _op(ctx):
        if delay:
            await asyncio.sleep(delay)
        return return_value
    return _op


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPipelineExecution:
    @pytest.mark.asyncio
    async def test_simple_pipeline_execution(self):
        """Three nodes in a chain execute successfully in order."""
        dag = DAG("simple")
        dag.add_node(Node(id="extract", operation=_make_sync_op([{"id": 1}, {"id": 2}])))
        dag.add_node(Node(id="transform", operation=_make_sync_op([{"id": 1, "v": "a"}])))
        dag.add_node(Node(id="load", operation=_make_sync_op(None)))
        dag.add_edge("extract", "transform")
        dag.add_edge("transform", "load")

        executor = PipelineExecutor(dag, max_parallel=2)
        state = await executor.execute()

        assert state.status == RunStatus.COMPLETED
        assert state.node_states["extract"].status == RunStatus.COMPLETED
        assert state.node_states["transform"].status == RunStatus.COMPLETED
        assert state.node_states["load"].status == RunStatus.COMPLETED
        assert state.node_states["extract"].output_records == 2

    @pytest.mark.asyncio
    async def test_parallel_execution(self):
        """Independent nodes can run concurrently."""
        execution_log: list[str] = []

        async def _log_op(name: str, delay: float):
            async def _inner(ctx):
                execution_log.append(f"{name}_start")
                await asyncio.sleep(delay)
                execution_log.append(f"{name}_end")
                return [{"node": name}]
            return _inner

        dag = DAG("parallel")
        dag.add_node(Node(id="root", operation=_make_sync_op([{"v": 1}])))
        dag.add_node(Node(id="branch_a", operation=await _log_op("a", 0.05)))
        dag.add_node(Node(id="branch_b", operation=await _log_op("b", 0.05)))
        dag.add_node(Node(id="merge", operation=_make_sync_op("done")))
        dag.add_edge("root", "branch_a")
        dag.add_edge("root", "branch_b")
        dag.add_edge("branch_a", "merge")
        dag.add_edge("branch_b", "merge")

        executor = PipelineExecutor(dag, max_parallel=4)
        state = await executor.execute()

        assert state.status == RunStatus.COMPLETED
        # Both branches should have started before either finishes
        a_start = execution_log.index("a_start")
        b_start = execution_log.index("b_start")
        a_end = execution_log.index("a_end")
        b_end = execution_log.index("b_end")
        assert a_start < a_end
        assert b_start < b_end

    @pytest.mark.asyncio
    async def test_failure_handling(self):
        """A node that raises an exception causes the pipeline to fail gracefully."""
        def _fail(ctx):
            raise RuntimeError("simulated failure")

        dag = DAG("fail_pipeline")
        dag.add_node(Node(id="ok", operation=_make_sync_op([1, 2, 3]), retries=0))
        dag.add_node(Node(id="bad", operation=_fail, retries=0))
        dag.add_edge("ok", "bad")

        executor = PipelineExecutor(dag, max_parallel=2)
        state = await executor.execute()

        assert state.status == RunStatus.FAILED
        assert state.node_states["ok"].status == RunStatus.COMPLETED
        assert state.node_states["bad"].status == RunStatus.FAILED
        assert "RuntimeError" in state.node_states["bad"].error

    @pytest.mark.asyncio
    async def test_retry_logic(self):
        """A node that fails once then succeeds uses one retry."""
        call_count = 0

        def _flaky(ctx):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("transient error")
            return ["ok"]

        dag = DAG("retry_test")
        dag.add_node(Node(id="flaky", operation=_flaky, retries=2))

        executor = PipelineExecutor(dag, max_parallel=1)
        state = await executor.execute()

        assert state.status == RunStatus.COMPLETED
        assert state.node_states["flaky"].status == RunStatus.COMPLETED
        assert call_count == 2
        assert state.node_states["flaky"].retries_used == 1

    @pytest.mark.asyncio
    async def test_on_node_complete_callback(self):
        """The on_node_complete callback fires for each node."""
        completed_nodes: list[str] = []

        def _callback(node_id, node_state):
            completed_nodes.append(node_id)

        dag = DAG("cb_test")
        dag.add_node(Node(id="a", operation=_make_sync_op("result_a")))
        dag.add_node(Node(id="b", operation=_make_sync_op("result_b")))
        dag.add_edge("a", "b")

        executor = PipelineExecutor(dag, max_parallel=2, on_node_complete=_callback)
        state = await executor.execute()

        assert state.status == RunStatus.COMPLETED
        assert "a" in completed_nodes
        assert "b" in completed_nodes

    @pytest.mark.asyncio
    async def test_results_available(self):
        """Executor.results contains outputs from all completed nodes."""
        dag = DAG("results_test")
        dag.add_node(Node(id="source", operation=_make_sync_op({"key": "value"})))

        executor = PipelineExecutor(dag, max_parallel=1)
        state = await executor.execute()

        assert state.status == RunStatus.COMPLETED
        assert executor.results["source"] == {"key": "value"}
