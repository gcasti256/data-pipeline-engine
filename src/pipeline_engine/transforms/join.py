from __future__ import annotations

from collections import defaultdict
from typing import Any

from pipeline_engine.transforms.base import BaseTransform

_VALID_JOIN_TYPES = frozenset({"inner", "left", "right", "outer"})


class JoinTransform(BaseTransform):
    """Hash-join two datasets on one or more key columns.

    Parameters
    ----------
    right_data:
        The right-hand dataset to join against.
    on:
        Column name(s) to join on.  A single string or a list of strings.
    how:
        Join type: ``"inner"``, ``"left"``, ``"right"``, or ``"outer"``.
    suffixes:
        Suffixes appended to overlapping column names from the left and right
        datasets respectively.
    """

    def __init__(
        self,
        right_data: list[dict[str, Any]],
        on: str | list[str],
        how: str = "inner",
        suffixes: tuple[str, str] = ("_left", "_right"),
        name: str = "",
    ) -> None:
        super().__init__(name=name)
        self._right_data = right_data
        self._on: list[str] = [on] if isinstance(on, str) else list(on)
        if how not in _VALID_JOIN_TYPES:
            raise ValueError(
                f"Unsupported join type {how!r}.  "
                f"Expected one of: {', '.join(sorted(_VALID_JOIN_TYPES))}"
            )
        self._how = how
        self._suffixes = suffixes

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def execute(
        self,
        data: list[dict[str, Any]],
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        self.validate_input(data)

        # Build a hash index on the right side keyed by the join columns.
        right_index: dict[tuple[Any, ...], list[dict[str, Any]]] = (
            defaultdict(list)
        )
        for record in self._right_data:
            key = self._make_key(record)
            right_index[key].append(record)

        # Determine overlapping non-key columns for suffix handling.
        left_cols = _all_columns(data)
        right_cols = _all_columns(self._right_data)
        key_set = set(self._on)
        overlap = (left_cols - key_set) & (right_cols - key_set)

        results: list[dict[str, Any]] = []
        matched_right_keys: set[tuple[Any, ...]] = set()

        for left_row in data:
            key = self._make_key(left_row)
            right_rows = right_index.get(key)

            if right_rows:
                matched_right_keys.add(key)
                for right_row in right_rows:
                    results.append(
                        self._merge(left_row, right_row, overlap)
                    )
            elif self._how in ("left", "outer"):
                results.append(
                    self._merge(left_row, None, overlap)
                )

        # For right / outer joins, emit unmatched right rows.
        if self._how in ("right", "outer"):
            for key, right_rows in right_index.items():
                if key not in matched_right_keys:
                    for right_row in right_rows:
                        results.append(
                            self._merge(None, right_row, overlap)
                        )

        return results

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _make_key(self, record: dict[str, Any]) -> tuple[Any, ...]:
        return tuple(record.get(col) for col in self._on)

    def _merge(
        self,
        left: dict[str, Any] | None,
        right: dict[str, Any] | None,
        overlap: set[str],
    ) -> dict[str, Any]:
        """Merge a left and right row, resolving column conflicts."""
        out: dict[str, Any] = {}
        left_suffix, right_suffix = self._suffixes
        key_set = set(self._on)

        # -- Key columns (from whichever side is available) --
        for col in self._on:
            if left is not None:
                out[col] = left.get(col)
            elif right is not None:
                out[col] = right.get(col)

        # -- Left columns --
        if left is not None:
            for col, val in left.items():
                if col in key_set:
                    continue
                out_col = f"{col}{left_suffix}" if col in overlap else col
                out[out_col] = val
        else:
            # Emit NULLs for left columns in right/outer unmatched rows.
            # We don't know the full schema, so skip — caller gets only
            # the key and right columns for unmatched right rows.
            pass

        # -- Right columns --
        if right is not None:
            for col, val in right.items():
                if col in key_set:
                    continue
                out_col = f"{col}{right_suffix}" if col in overlap else col
                out[out_col] = val

        return out


def _all_columns(data: list[dict[str, Any]]) -> set[str]:
    """Collect the union of all column names across records."""
    cols: set[str] = set()
    for record in data:
        cols.update(record.keys())
    return cols
