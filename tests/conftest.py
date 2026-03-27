"""Shared fixtures for the pipeline engine test suite."""

from __future__ import annotations

import pytest

from pipeline_engine.core.dag import DAG, Node


def _noop(ctx):
    """No-op operation for test nodes."""
    return ctx.get("config", {})


@pytest.fixture
def simple_dag() -> DAG:
    """A 3-node linear DAG: extract -> transform -> load."""
    dag = DAG("test_pipeline")
    dag.add_node(Node(id="extract", operation=_noop))
    dag.add_node(Node(id="transform", operation=_noop))
    dag.add_node(Node(id="load", operation=_noop))
    dag.add_edge("extract", "transform")
    dag.add_edge("transform", "load")
    return dag


@pytest.fixture
def diamond_dag() -> DAG:
    """A diamond-shaped DAG: A -> B, A -> C, B -> D, C -> D."""
    dag = DAG("diamond")
    dag.add_node(Node(id="A", operation=_noop))
    dag.add_node(Node(id="B", operation=_noop))
    dag.add_node(Node(id="C", operation=_noop))
    dag.add_node(Node(id="D", operation=_noop))
    dag.add_edge("A", "B")
    dag.add_edge("A", "C")
    dag.add_edge("B", "D")
    dag.add_edge("C", "D")
    return dag


@pytest.fixture
def sample_records() -> list[dict]:
    """A reusable set of sample records for transform tests."""
    return [
        {"name": "Alice", "age": 30, "city": "New York", "salary": 70000},
        {"name": "Bob", "age": 25, "city": "Chicago", "salary": 60000},
        {"name": "Charlie", "age": 35, "city": "New York", "salary": 80000},
        {"name": "Diana", "age": 28, "city": "Chicago", "salary": 65000},
        {"name": "Eve", "age": 32, "city": "Boston", "salary": 75000},
    ]
