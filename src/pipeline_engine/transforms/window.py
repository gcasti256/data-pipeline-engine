from __future__ import annotations

from typing import Any

from pipeline_engine.transforms.base import BaseTransform

_SUPPORTED_AGGREGATIONS = frozenset({"sum", "avg", "min", "max", "count"})


class WindowTransform(BaseTransform):
    """Apply a sliding-window aggregation over a column.

    For each window position a new column
    ``{column}_window_{aggregation}`` is added to the record at the *last*
    position of the window.  Records that fall before the first complete window
    receive ``None`` for the window column.

    Parameters
    ----------
    size:
        Number of records in each window.
    step:
        How many records to advance between successive windows.  Defaults to 1
        (fully overlapping windows).
    aggregation:
        Aggregation function: ``"sum"``, ``"avg"``, ``"min"``, ``"max"``,
        or ``"count"``.
    column:
        Source column to aggregate.  Required for all aggregations except
        ``"count"``.
    """

    def __init__(
        self,
        size: int,
        step: int = 1,
        aggregation: str = "avg",
        column: str = "",
        name: str = "",
    ) -> None:
        super().__init__(name=name)
        if size < 1:
            raise ValueError(f"Window size must be >= 1, got {size}")
        if step < 1:
            raise ValueError(f"Step must be >= 1, got {step}")
        if aggregation not in _SUPPORTED_AGGREGATIONS:
            raise ValueError(
                f"Unsupported aggregation {aggregation!r}.  "
                f"Expected one of: {', '.join(sorted(_SUPPORTED_AGGREGATIONS))}"
            )
        if aggregation != "count" and not column:
            raise ValueError(
                f"Aggregation {aggregation!r} requires a column argument"
            )
        self._size = size
        self._step = step
        self._aggregation = aggregation
        self._column = column


    def execute(
        self,
        data: list[dict[str, Any]],
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        self.validate_input(data)

        out_col = f"{self._column}_window_{self._aggregation}"
        n = len(data)

        # Pre-fill every record with None for the window column.
        results: list[dict[str, Any]] = [
            {**record, out_col: None} for record in data
        ]

        # Slide the window across the data.
        pos = 0
        while pos + self._size <= n:
            window_records = data[pos : pos + self._size]
            agg_value = self._compute(window_records)
            # Attach the aggregated value to the last record in the window.
            results[pos + self._size - 1][out_col] = agg_value
            pos += self._step

        return results


    def _compute(self, window: list[dict[str, Any]]) -> Any:
        if self._aggregation == "count":
            return len(window)

        values: list[Any] = [
            r[self._column]
            for r in window
            if self._column in r and r[self._column] is not None
        ]

        if not values:
            return None

        if self._aggregation == "sum":
            return sum(values)
        if self._aggregation == "avg":
            return sum(values) / len(values)
        if self._aggregation == "min":
            return min(values)
        if self._aggregation == "max":
            return max(values)

        raise ValueError(f"Unhandled aggregation: {self._aggregation!r}")
