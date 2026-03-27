from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

from pipeline_engine.transforms.base import BaseTransform

_AGG_RE = re.compile(r"^(\w+)\((\w*)\)$")


class AggregateTransform(BaseTransform):
    """Group records and compute aggregate values.

    Parameters
    ----------
    group_by:
        Column names to group on.
    aggregations:
        Mapping of ``{output_column: "func(source_column)"}``.
        Supported functions: ``sum``, ``avg``, ``count``, ``min``, ``max``,
        ``first``, ``last``.  ``count()`` may omit the column name.
    """

    _AGG_FUNCTIONS: dict[str, str] = {
        "sum", "avg", "count", "min", "max", "first", "last",  # type: ignore[assignment]
    }

    def __init__(
        self,
        group_by: list[str],
        aggregations: dict[str, str],
        name: str = "",
    ) -> None:
        super().__init__(name=name)
        self._group_by = group_by
        self._aggregations = aggregations
        self._parsed: list[tuple[str, str, str | None]] = []
        for out_col, expr in aggregations.items():
            func, col = self._parse_aggregation(expr)
            self._parsed.append((out_col, func, col))

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_aggregation(expr: str) -> tuple[str, str | None]:
        """Parse ``"func(column)"`` into ``(func, column)``."""
        m = _AGG_RE.match(expr.strip())
        if m is None:
            raise ValueError(
                f"Invalid aggregation expression: {expr!r}.  "
                "Expected format: func(column)"
            )
        func = m.group(1).lower()
        col: str | None = m.group(2) or None
        supported = {"sum", "avg", "count", "min", "max", "first", "last"}
        if func not in supported:
            raise ValueError(
                f"Unsupported aggregation function: {func!r}.  "
                f"Supported: {', '.join(sorted(supported))}"
            )
        if func != "count" and col is None:
            raise ValueError(
                f"Aggregation function {func!r} requires a column argument"
            )
        return func, col

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def execute(
        self,
        data: list[dict[str, Any]],
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        self.validate_input(data)

        groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
        for record in data:
            key = tuple(record.get(col) for col in self._group_by)
            groups[key].append(record)

        results: list[dict[str, Any]] = []
        for key, rows in groups.items():
            row: dict[str, Any] = {}
            # Include group-by columns.
            for col_name, value in zip(self._group_by, key):
                row[col_name] = value
            # Compute aggregations.
            for out_col, func, src_col in self._parsed:
                row[out_col] = self._aggregate(func, src_col, rows)
            results.append(row)

        return results

    @staticmethod
    def _aggregate(
        func: str,
        column: str | None,
        rows: list[dict[str, Any]],
    ) -> Any:
        """Compute a single aggregation over *rows*."""
        if func == "count":
            return len(rows)

        assert column is not None  # enforced at parse time
        values: list[Any] = [
            r[column] for r in rows if column in r and r[column] is not None
        ]

        if not values:
            return None

        if func == "sum":
            return sum(values)
        if func == "avg":
            return sum(values) / len(values)
        if func == "min":
            return min(values)
        if func == "max":
            return max(values)
        if func == "first":
            return values[0]
        if func == "last":
            return values[-1]

        raise ValueError(f"Unhandled aggregation: {func!r}")
