"""Directed acyclic graph for pipeline topology definition and validation."""

from __future__ import annotations

import logging
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


class CycleError(Exception):
    """Raised when a cycle is detected in the DAG."""

    def __init__(self, involved_nodes: list[str] | None = None) -> None:
        self.involved_nodes = involved_nodes or []
        nodes_desc = ", ".join(self.involved_nodes) if self.involved_nodes else "unknown"
        super().__init__(f"Cycle detected involving nodes: {nodes_desc}")


class DAGValidationError(Exception):
    """Raised when DAG validation fails with one or more structural errors."""

    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        summary = "; ".join(errors)
        super().__init__(f"DAG validation failed: {summary}")


@dataclass(frozen=True, slots=True)
class Node:
    """A single operation within a pipeline DAG.

    Attributes:
        id: Unique identifier for this node within the DAG.
        operation: Callable that performs the node's work. Receives a context dict
            and returns output data.
        config: Arbitrary configuration passed to the operation at runtime.
        retries: Maximum number of retry attempts on failure (0 = no retries).
        timeout: Per-execution timeout in seconds, or None for no limit.
    """

    id: str
    operation: Callable[..., Any]
    config: dict[str, Any] = field(default_factory=dict)
    retries: int = 3
    timeout: float | None = None

    def __post_init__(self) -> None:
        if not self.id:
            raise ValueError("Node id must be a non-empty string")
        if not callable(self.operation):
            raise TypeError(f"Node operation must be callable, got {type(self.operation).__name__}")
        if self.retries < 0:
            raise ValueError(f"Node retries must be >= 0, got {self.retries}")
        if self.timeout is not None and self.timeout <= 0:
            raise ValueError(f"Node timeout must be positive, got {self.timeout}")


class DAG:
    """Directed acyclic graph representing a data pipeline's execution topology.

    Nodes represent operations; directed edges encode dependency relationships
    where an edge (A, B) means "A must complete before B can start."

    Example::

        dag = DAG("etl_pipeline")
        dag.add_node(Node(id="extract", operation=extract_fn))
        dag.add_node(Node(id="transform", operation=transform_fn))
        dag.add_node(Node(id="load", operation=load_fn))
        dag.add_edge("extract", "transform")
        dag.add_edge("transform", "load")

        order = dag.topological_sort()  # ["extract", "transform", "load"]
    """

    def __init__(self, name: str) -> None:
        if not name:
            raise ValueError("DAG name must be a non-empty string")
        self.name = name
        self._nodes: dict[str, Node] = {}
        self._edges: list[tuple[str, str]] = []
        # Adjacency structures kept in sync for O(1) lookups.
        self._forward: dict[str, set[str]] = {}  # node -> direct successors
        self._reverse: dict[str, set[str]] = {}  # node -> direct predecessors


    def add_node(self, node: Node) -> None:
        """Register a node in the DAG.

        Raises:
            ValueError: If a node with the same id already exists.
        """
        if node.id in self._nodes:
            raise ValueError(f"Duplicate node id: '{node.id}'")
        self._nodes[node.id] = node
        self._forward.setdefault(node.id, set())
        self._reverse.setdefault(node.id, set())
        logger.debug("Added node '%s' to DAG '%s'", node.id, self.name)

    def add_edge(self, from_id: str, to_id: str) -> None:
        """Add a directed dependency edge: *from_id* must complete before *to_id*.

        Raises:
            KeyError: If either node id is not present in the DAG.
            ValueError: If the edge would create a self-loop or duplicate.
        """
        if from_id not in self._nodes:
            raise KeyError(f"Source node '{from_id}' not found in DAG")
        if to_id not in self._nodes:
            raise KeyError(f"Target node '{to_id}' not found in DAG")
        if from_id == to_id:
            raise ValueError(f"Self-loop not allowed: '{from_id}'")
        if to_id in self._forward[from_id]:
            raise ValueError(f"Duplicate edge: '{from_id}' -> '{to_id}'")

        self._edges.append((from_id, to_id))
        self._forward[from_id].add(to_id)
        self._reverse[to_id].add(from_id)
        logger.debug("Added edge '%s' -> '%s' in DAG '%s'", from_id, to_id, self.name)


    @property
    def nodes(self) -> dict[str, Node]:
        """Read-only view of registered nodes."""
        return dict(self._nodes)

    @property
    def edges(self) -> list[tuple[str, str]]:
        """Copy of the edge list."""
        return list(self._edges)

    def get_dependencies(self, node_id: str) -> set[str]:
        """Return the set of direct predecessors (upstream dependencies) of *node_id*.

        Raises:
            KeyError: If *node_id* is not in the DAG.
        """
        if node_id not in self._nodes:
            raise KeyError(f"Node '{node_id}' not found in DAG")
        return set(self._reverse[node_id])

    def get_dependents(self, node_id: str) -> set[str]:
        """Return the set of direct successors (downstream dependents) of *node_id*.

        Raises:
            KeyError: If *node_id* is not in the DAG.
        """
        if node_id not in self._nodes:
            raise KeyError(f"Node '{node_id}' not found in DAG")
        return set(self._forward[node_id])

    def get_roots(self) -> set[str]:
        """Return nodes with zero incoming edges (entry points of the pipeline)."""
        return {nid for nid, preds in self._reverse.items() if not preds}


    def topological_sort(self) -> list[str]:
        """Return a valid topological execution order using Kahn's algorithm.

        Raises:
            CycleError: If the graph contains a cycle.
        """
        in_degree: dict[str, int] = {nid: len(preds) for nid, preds in self._reverse.items()}
        queue: deque[str] = deque(nid for nid, deg in in_degree.items() if deg == 0)
        order: list[str] = []

        while queue:
            current = queue.popleft()
            order.append(current)
            for successor in sorted(self._forward[current]):  # sorted for determinism
                in_degree[successor] -= 1
                if in_degree[successor] == 0:
                    queue.append(successor)

        if len(order) != len(self._nodes):
            # Identify the nodes trapped in the cycle for diagnostics.
            cycle_nodes = [nid for nid in self._nodes if nid not in order]
            raise CycleError(involved_nodes=sorted(cycle_nodes))

        return order


    def validate(self) -> list[str]:
        """Validate the DAG structure.

        Returns a list of warning strings (empty if clean). Raises on hard errors.

        Raises:
            CycleError: If the graph contains a cycle.
            DAGValidationError: If there are structural errors that prevent execution.
        """
        errors: list[str] = []
        warnings: list[str] = []

        # No nodes at all is an error.
        if not self._nodes:
            errors.append("DAG has no nodes")

        # Cycle detection (raises CycleError directly).
        self.topological_sort()

        # Warn about orphan nodes (no edges at all — neither in nor out).
        for nid in self._nodes:
            has_incoming = bool(self._reverse.get(nid))
            has_outgoing = bool(self._forward.get(nid))
            if not has_incoming and not has_outgoing and len(self._nodes) > 1:
                warnings.append(f"Node '{nid}' is disconnected (no incoming or outgoing edges)")

        if errors:
            raise DAGValidationError(errors)

        return warnings


    def __len__(self) -> int:
        return len(self._nodes)

    def __contains__(self, node_id: str) -> bool:
        return node_id in self._nodes

    def __repr__(self) -> str:
        return f"DAG(name={self.name!r}, nodes={len(self._nodes)}, edges={len(self._edges)})"
