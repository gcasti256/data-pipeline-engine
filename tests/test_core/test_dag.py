"""Tests for pipeline_engine.core.dag — DAG construction, validation, and traversal."""

from __future__ import annotations

import pytest

from pipeline_engine.core.dag import DAG, CycleError, DAGValidationError, Node


def _noop(ctx):
    return None


class TestNode:
    def test_create_valid_node(self):
        node = Node(id="extract", operation=_noop)
        assert node.id == "extract"
        assert node.retries == 3
        assert node.timeout is None
        assert node.config == {}

    def test_empty_id_raises(self):
        with pytest.raises(ValueError, match="non-empty string"):
            Node(id="", operation=_noop)

    def test_non_callable_operation_raises(self):
        with pytest.raises(TypeError, match="callable"):
            Node(id="x", operation="not_callable")

    def test_negative_retries_raises(self):
        with pytest.raises(ValueError, match="retries must be >= 0"):
            Node(id="x", operation=_noop, retries=-1)

    def test_non_positive_timeout_raises(self):
        with pytest.raises(ValueError, match="timeout must be positive"):
            Node(id="x", operation=_noop, timeout=0)


class TestDAGMutation:
    def test_add_node(self):
        dag = DAG("test")
        dag.add_node(Node(id="a", operation=_noop))
        assert "a" in dag
        assert len(dag) == 1

    def test_add_duplicate_node_raises(self):
        dag = DAG("test")
        dag.add_node(Node(id="a", operation=_noop))
        with pytest.raises(ValueError, match="Duplicate node id"):
            dag.add_node(Node(id="a", operation=_noop))

    def test_add_edge(self):
        dag = DAG("test")
        dag.add_node(Node(id="a", operation=_noop))
        dag.add_node(Node(id="b", operation=_noop))
        dag.add_edge("a", "b")
        assert dag.edges == [("a", "b")]

    def test_add_edge_missing_source_raises(self):
        dag = DAG("test")
        dag.add_node(Node(id="b", operation=_noop))
        with pytest.raises(KeyError, match="Source node 'a' not found"):
            dag.add_edge("a", "b")

    def test_add_edge_missing_target_raises(self):
        dag = DAG("test")
        dag.add_node(Node(id="a", operation=_noop))
        with pytest.raises(KeyError, match="Target node 'b' not found"):
            dag.add_edge("a", "b")

    def test_self_loop_raises(self):
        dag = DAG("test")
        dag.add_node(Node(id="a", operation=_noop))
        with pytest.raises(ValueError, match="Self-loop"):
            dag.add_edge("a", "a")

    def test_duplicate_edge_raises(self):
        dag = DAG("test")
        dag.add_node(Node(id="a", operation=_noop))
        dag.add_node(Node(id="b", operation=_noop))
        dag.add_edge("a", "b")
        with pytest.raises(ValueError, match="Duplicate edge"):
            dag.add_edge("a", "b")


class TestTopologicalSort:
    def test_simple_chain(self, simple_dag: DAG):
        order = simple_dag.topological_sort()
        assert order == ["extract", "transform", "load"]

    def test_diamond_dependency(self, diamond_dag: DAG):
        order = diamond_dag.topological_sort()
        # A must come first, D must come last; B and C can be in any order
        assert order[0] == "A"
        assert order[-1] == "D"
        assert set(order[1:3]) == {"B", "C"}

    def test_cycle_detection(self):
        dag = DAG("cyclic")
        dag.add_node(Node(id="a", operation=_noop))
        dag.add_node(Node(id="b", operation=_noop))
        dag.add_node(Node(id="c", operation=_noop))
        dag.add_edge("a", "b")
        dag.add_edge("b", "c")
        dag.add_edge("c", "a")
        with pytest.raises(CycleError) as exc_info:
            dag.topological_sort()
        assert set(exc_info.value.involved_nodes) == {"a", "b", "c"}

    def test_single_node(self):
        dag = DAG("single")
        dag.add_node(Node(id="only", operation=_noop))
        assert dag.topological_sort() == ["only"]


class TestDAGQueries:
    def test_get_roots(self, simple_dag: DAG):
        assert simple_dag.get_roots() == {"extract"}

    def test_get_roots_diamond(self, diamond_dag: DAG):
        assert diamond_dag.get_roots() == {"A"}

    def test_get_dependencies(self, diamond_dag: DAG):
        deps = diamond_dag.get_dependencies("D")
        assert deps == {"B", "C"}

    def test_get_dependencies_root(self, diamond_dag: DAG):
        assert diamond_dag.get_dependencies("A") == set()

    def test_get_dependencies_unknown_node_raises(self, simple_dag: DAG):
        with pytest.raises(KeyError, match="not found"):
            simple_dag.get_dependencies("unknown")

    def test_get_dependents(self, diamond_dag: DAG):
        assert diamond_dag.get_dependents("A") == {"B", "C"}


class TestDAGValidation:
    def test_validate_valid_dag(self, simple_dag: DAG):
        warnings = simple_dag.validate()
        assert warnings == []

    def test_validate_empty_dag_raises(self):
        dag = DAG("empty")
        with pytest.raises(DAGValidationError, match="no nodes"):
            dag.validate()

    def test_validate_cycle_raises(self):
        dag = DAG("cyclic")
        dag.add_node(Node(id="a", operation=_noop))
        dag.add_node(Node(id="b", operation=_noop))
        dag.add_edge("a", "b")
        dag.add_edge("b", "a")
        with pytest.raises(CycleError):
            dag.validate()

    def test_validate_warns_disconnected_node(self):
        dag = DAG("test")
        dag.add_node(Node(id="a", operation=_noop))
        dag.add_node(Node(id="b", operation=_noop))
        dag.add_node(Node(id="orphan", operation=_noop))
        dag.add_edge("a", "b")
        warnings = dag.validate()
        assert any("orphan" in w and "disconnected" in w for w in warnings)

    def test_empty_dag_name_raises(self):
        with pytest.raises(ValueError, match="non-empty string"):
            DAG("")
