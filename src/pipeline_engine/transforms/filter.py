from __future__ import annotations

import re
from typing import Any

from pipeline_engine.transforms.base import BaseTransform

# Operators ordered longest-first so that ``>=`` is tried before ``>``.
_COMPARISON_OPS: list[str] = [">=", "<=", "!=", "==", ">", "<"]
_MEMBERSHIP_RE = re.compile(
    r"^(\w+)\s+in\s+\[(.+)\]$", re.IGNORECASE
)
_REGEX_RE = re.compile(r"^(\w+)\s*~\s*'(.+)'$")


class FilterTransform(BaseTransform):
    """Filter records that match a simple condition expression.

    Supported condition forms:

    * ``"amount > 100"``          — numeric comparison (>, <, >=, <=, ==, !=)
    * ``"status == 'active'"``    — string equality
    * ``"category in ['A', 'B']"`` — membership test
    * ``"name ~ '^John'"``        — regex match using ``~`` operator
    """

    def __init__(self, condition: str, name: str = "") -> None:
        super().__init__(name=name)
        self._raw_condition: str = condition.strip()
        self._field: str
        self._op: str
        self._value: Any
        self._field, self._op, self._value = self._parse_condition(
            self._raw_condition
        )

    # ------------------------------------------------------------------
    # Condition parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_condition(condition: str) -> tuple[str, str, Any]:
        """Parse a human-readable condition string into (field, op, value).

        Returns a 3-tuple suitable for ``_evaluate``.
        """
        # --- Membership: field in ['a', 'b', 'c'] ---
        m = _MEMBERSHIP_RE.match(condition)
        if m is not None:
            field = m.group(1).strip()
            raw_items = m.group(2)
            values = _parse_list_literal(raw_items)
            return field, "in", values

        # --- Regex: field ~ 'pattern' ---
        m = _REGEX_RE.match(condition)
        if m is not None:
            field = m.group(1).strip()
            pattern = m.group(2)
            return field, "~", re.compile(pattern)

        # --- Comparison: field op value ---
        for op in _COMPARISON_OPS:
            if op in condition:
                parts = condition.split(op, maxsplit=1)
                if len(parts) == 2:
                    field = parts[0].strip()
                    raw_value = parts[1].strip()
                    value = _coerce_value(raw_value)
                    return field, op, value

        raise ValueError(f"Unable to parse condition: {condition!r}")

    # ------------------------------------------------------------------
    # Evaluation
    # ------------------------------------------------------------------

    @staticmethod
    def _evaluate(
        record: dict[str, Any],
        field: str,
        op: str,
        value: Any,
    ) -> bool:
        """Return *True* if *record* satisfies the condition."""
        record_value = record.get(field)
        if record_value is None:
            return False

        if op == "in":
            return record_value in value

        if op == "~":
            return value.search(str(record_value)) is not None

        # Numeric / string comparisons
        try:
            left: Any = record_value
            right: Any = value
            # Promote to float when both sides look numeric.
            if isinstance(left, str):
                left = float(left)
            if isinstance(right, str):
                right = float(right)
        except (ValueError, TypeError):
            left = record_value
            right = value

        if op == ">":
            return left > right  # type: ignore[operator]
        if op == "<":
            return left < right  # type: ignore[operator]
        if op == ">=":
            return left >= right  # type: ignore[operator]
        if op == "<=":
            return left <= right  # type: ignore[operator]
        if op == "==":
            return left == right  # type: ignore[operator]
        if op == "!=":
            return left != right  # type: ignore[operator]

        raise ValueError(f"Unsupported operator: {op!r}")

    # ------------------------------------------------------------------
    # Execute
    # ------------------------------------------------------------------

    def execute(
        self,
        data: list[dict[str, Any]],
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        self.validate_input(data)
        return [
            record
            for record in data
            if self._evaluate(record, self._field, self._op, self._value)
        ]


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _coerce_value(raw: str) -> Any:
    """Coerce a string literal into a Python value.

    Handles quoted strings, ints, floats, booleans, and ``None``.
    """
    # Quoted string
    if (raw.startswith("'") and raw.endswith("'")) or (
        raw.startswith('"') and raw.endswith('"')
    ):
        return raw[1:-1]

    low = raw.lower()
    if low == "none" or low == "null":
        return None
    if low == "true":
        return True
    if low == "false":
        return False

    # Numeric
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        pass

    return raw


def _parse_list_literal(raw: str) -> list[Any]:
    """Parse the inner part of ``[a, b, c]`` into a list of values."""
    items: list[Any] = []
    for part in raw.split(","):
        items.append(_coerce_value(part.strip()))
    return items
