"""Custom validation rules for flexible, composable record validation."""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

from pipeline_engine.validation.schema import ValidationResult


class ValidationRule(ABC):
    """Abstract base class for a single validation rule.

    Subclasses must implement :meth:`check`, which inspects a single record
    and returns a ``(passed, message)`` tuple.
    """

    def __init__(self, name: str = "") -> None:
        self.name: str = name or self.__class__.__name__

    @abstractmethod
    def check(self, record: dict[str, Any]) -> tuple[bool, str]:
        """Check whether *record* satisfies this rule.

        Returns:
            A tuple of ``(passed, message)``.  When *passed* is ``True``,
            *message* is typically empty.  When ``False``, *message* describes
            the violation.
        """
        ...

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r})"


class RangeRule(ValidationRule):
    """Validate that a numeric field falls within an inclusive range.

    Parameters
    ----------
    field:
        The record key to check.
    min_val:
        Minimum acceptable value (inclusive).
    max_val:
        Maximum acceptable value (inclusive).
    """

    def __init__(
        self,
        field: str,
        min_val: float,
        max_val: float,
        name: str = "",
    ) -> None:
        super().__init__(name=name or f"RangeRule({field})")
        self._field = field
        self._min_val = min_val
        self._max_val = max_val

    def check(self, record: dict[str, Any]) -> tuple[bool, str]:
        value = record.get(self._field)
        if value is None:
            return False, f"Field '{self._field}' is missing or None"
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return False, (
                f"Field '{self._field}' is not numeric: {value!r}"
            )
        if numeric < self._min_val or numeric > self._max_val:
            return False, (
                f"Field '{self._field}' value {numeric} is outside "
                f"range [{self._min_val}, {self._max_val}]"
            )
        return True, ""


class NotNullRule(ValidationRule):
    """Validate that specified fields are not None and not empty strings.

    Parameters
    ----------
    fields:
        List of record keys that must contain non-null, non-empty values.
    """

    def __init__(self, fields: list[str], name: str = "") -> None:
        super().__init__(name=name or f"NotNullRule({', '.join(fields)})")
        if not fields:
            raise ValueError("At least one field is required")
        self._fields = fields

    def check(self, record: dict[str, Any]) -> tuple[bool, str]:
        missing: list[str] = []
        for field in self._fields:
            value = record.get(field)
            if value is None or value == "":
                missing.append(field)
        if missing:
            return False, (
                f"Fields must not be null or empty: {', '.join(missing)}"
            )
        return True, ""


class RegexRule(ValidationRule):
    """Validate that a string field matches a regular expression.

    Parameters
    ----------
    field:
        The record key to check.
    pattern:
        A regular expression pattern.  The full field value must match
        (the pattern is anchored with ``re.fullmatch``).
    """

    def __init__(self, field: str, pattern: str, name: str = "") -> None:
        super().__init__(name=name or f"RegexRule({field})")
        self._field = field
        self._pattern = pattern
        self._compiled = re.compile(pattern)

    def check(self, record: dict[str, Any]) -> tuple[bool, str]:
        value = record.get(self._field)
        if value is None:
            return False, f"Field '{self._field}' is missing or None"
        if not isinstance(value, str):
            value = str(value)
        if not self._compiled.fullmatch(value):
            return False, (
                f"Field '{self._field}' value {value!r} does not match "
                f"pattern '{self._pattern}'"
            )
        return True, ""


class CustomRule(ValidationRule):
    """User-defined validation rule backed by a callable.

    Parameters
    ----------
    name:
        A descriptive rule name.
    check_fn:
        A callable accepting a record dict and returning
        ``(passed: bool, message: str)``.
    """

    def __init__(
        self,
        name: str,
        check_fn: Callable[[dict[str, Any]], tuple[bool, str]],
    ) -> None:
        super().__init__(name=name)
        if not callable(check_fn):
            raise TypeError("check_fn must be callable")
        self._check_fn = check_fn

    def check(self, record: dict[str, Any]) -> tuple[bool, str]:
        return self._check_fn(record)


class RuleSet:
    """A collection of :class:`ValidationRule` instances applied in sequence.

    Every rule is checked against each record.  A record is considered
    *invalid* if **any** rule fails.  All failure messages for a given
    record are aggregated into a single error string.

    Example::

        rules = RuleSet([
            NotNullRule(["name", "email"]),
            RangeRule("age", 0, 150),
        ])
        result = rules.validate(records)
    """

    def __init__(self, rules: list[ValidationRule] | None = None) -> None:
        self._rules: list[ValidationRule] = list(rules) if rules else []

    @property
    def rules(self) -> list[ValidationRule]:
        """Read-only copy of the current rules."""
        return list(self._rules)

    def add(self, rule: ValidationRule) -> None:
        """Append a rule to the set."""
        self._rules.append(rule)

    def validate(self, records: list[dict[str, Any]]) -> ValidationResult:
        """Apply all rules to every record and partition results.

        Args:
            records: List of record dicts to validate.

        Returns:
            A :class:`ValidationResult` with valid and invalid partitions.
        """
        result = ValidationResult()

        for record in records:
            failures: list[str] = []
            for rule in self._rules:
                passed, message = rule.check(record)
                if not passed:
                    failures.append(f"[{rule.name}] {message}")

            if failures:
                result.invalid.append((record, "; ".join(failures)))
            else:
                result.valid.append(record)

        return result

    def __len__(self) -> int:
        return len(self._rules)

    def __repr__(self) -> str:
        return f"RuleSet(rules={len(self._rules)})"
