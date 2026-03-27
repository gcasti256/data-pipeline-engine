from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BaseTransform(ABC):
    """Abstract base class for data transformations.

    All transforms accept a list of record dicts, apply some operation, and
    return a (possibly different-length) list of record dicts.  Subclasses must
    implement ``execute``; they may optionally override ``validate_input`` for
    domain-specific pre-checks.
    """

    def __init__(self, name: str = "", **kwargs: Any) -> None:
        self.name: str = name or self.__class__.__name__
        self._config: dict[str, Any] = kwargs

    @abstractmethod
    def execute(
        self,
        data: list[dict[str, Any]],
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Transform the input data and return transformed records."""
        ...

    def validate_input(self, data: list[dict[str, Any]]) -> None:
        """Validate input data before transformation.

        Override in subclasses for custom validation logic.  The default
        implementation only checks that *data* is a ``list``.
        """
        if not isinstance(data, list):
            raise TypeError(
                f"Expected list of dicts, got {type(data).__name__}"
            )

    def __repr__(self) -> str:  # pragma: no cover – convenience
        return f"{self.__class__.__name__}(name={self.name!r})"
