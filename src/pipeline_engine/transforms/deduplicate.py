from __future__ import annotations

from typing import Any

from pipeline_engine.transforms.base import BaseTransform

_VALID_KEEP = frozenset({"first", "last"})


class DeduplicateTransform(BaseTransform):
    """Remove duplicate records based on one or more key columns.

    Parameters
    ----------
    keys:
        Column names that together define the uniqueness constraint.
    keep:
        Which duplicate to retain: ``"first"`` (default) keeps the earliest
        occurrence, ``"last"`` keeps the latest.
    """

    def __init__(
        self,
        keys: list[str],
        keep: str = "first",
        name: str = "",
    ) -> None:
        super().__init__(name=name)
        if not keys:
            raise ValueError("At least one key column is required")
        if keep not in _VALID_KEEP:
            raise ValueError(
                f"Unsupported keep strategy {keep!r}.  "
                f"Expected one of: {', '.join(sorted(_VALID_KEEP))}"
            )
        self._keys = keys
        self._keep = keep

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def execute(
        self,
        data: list[dict[str, Any]],
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        self.validate_input(data)

        if self._keep == "first":
            return self._dedup_first(data)
        return self._dedup_last(data)

    # ------------------------------------------------------------------
    # Strategies
    # ------------------------------------------------------------------

    def _make_key(self, record: dict[str, Any]) -> tuple[Any, ...]:
        return tuple(record.get(k) for k in self._keys)

    def _dedup_first(
        self, data: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Keep the first occurrence of each key combination."""
        seen: set[tuple[Any, ...]] = set()
        results: list[dict[str, Any]] = []
        for record in data:
            key = self._make_key(record)
            if key not in seen:
                seen.add(key)
                results.append(record)
        return results

    def _dedup_last(
        self, data: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Keep the last occurrence of each key combination.

        We iterate in reverse to find the last occurrence, then reverse
        the result to preserve original ordering.
        """
        seen: set[tuple[Any, ...]] = set()
        results: list[dict[str, Any]] = []
        for record in reversed(data):
            key = self._make_key(record)
            if key not in seen:
                seen.add(key)
                results.append(record)
        results.reverse()
        return results
