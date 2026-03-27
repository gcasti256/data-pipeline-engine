"""Tests for pipeline_engine.core.scheduler — dependency-aware batch dispatch."""

from __future__ import annotations

import pytest

from pipeline_engine.core.dag import DAG, Node
from pipeline_engine.core.scheduler import Scheduler
from pipeline_engine.core.state import PipelineState


def _noop(ctx):
    return None


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestScheduler:
    def test_get_next_batch_respects_dependencies(self, diamond_dag: DAG):
        """Only nodes with all dependencies COMPLETED are dispatched."""
        state = PipelineState("diamond")
        state.initialize(diamond_dag)
        scheduler = Scheduler(diamond_dag, state, max_parallel=4)

        # Initially only A (root) should be dispatchable.
        batch = scheduler.get_next_batch()
        assert batch == ["A"]

        # Simulate A completing.
        state.mark_running("A")
        state.mark_completed("A")

        # Now B and C should be ready (both depend only on A).
        batch = scheduler.get_next_batch()
        assert set(batch) == {"B", "C"}

        # Simulate B completing; D still needs C.
        state.mark_running("B")
        state.mark_completed("B")
        batch = scheduler.get_next_batch()
        assert batch == ["C"]  # C is still pending

        # Complete C, now D becomes available.
        state.mark_running("C")
        state.mark_completed("C")
        batch = scheduler.get_next_batch()
        assert batch == ["D"]

    def test_max_parallel_limits_batch_size(self):
        """The scheduler respects the max_parallel cap."""
        dag = DAG("wide")
        for name in ["a", "b", "c", "d"]:
            dag.add_node(Node(id=name, operation=_noop))

        state = PipelineState("wide")
        state.initialize(dag)
        scheduler = Scheduler(dag, state, max_parallel=2)

        batch = scheduler.get_next_batch()
        assert len(batch) == 2

    def test_is_complete_all_done(self, simple_dag: DAG):
        """is_complete returns True when all nodes are COMPLETED."""
        state = PipelineState("test")
        state.initialize(simple_dag)
        scheduler = Scheduler(simple_dag, state)

        assert scheduler.is_complete() is False

        for node_id in simple_dag.topological_sort():
            state.mark_running(node_id)
            state.mark_completed(node_id)

        assert scheduler.is_complete() is True

    def test_has_failed(self, simple_dag: DAG):
        """has_failed returns True when any node has FAILED status."""
        state = PipelineState("test")
        state.initialize(simple_dag)
        scheduler = Scheduler(simple_dag, state)

        assert scheduler.has_failed() is False

        state.mark_running("extract")
        state.mark_failed("extract", error="test error")

        assert scheduler.has_failed() is True
        # is_complete is also True when failed
        assert scheduler.is_complete() is True

    def test_no_dispatch_after_failure(self, simple_dag: DAG):
        """After a node fails, get_next_batch returns empty."""
        state = PipelineState("test")
        state.initialize(simple_dag)
        scheduler = Scheduler(simple_dag, state)

        state.mark_running("extract")
        state.mark_failed("extract", error="boom")

        assert scheduler.get_next_batch() == []

    def test_invalid_max_parallel_raises(self, simple_dag: DAG):
        state = PipelineState("test")
        state.initialize(simple_dag)
        with pytest.raises(ValueError, match="max_parallel must be >= 1"):
            Scheduler(simple_dag, state, max_parallel=0)
