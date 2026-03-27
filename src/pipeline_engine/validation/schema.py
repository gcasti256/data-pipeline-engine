"""Pydantic-based schema validation for pipeline records."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, ValidationError


@dataclass
class ValidationResult:
    """Result of validating a batch of records.

    Attributes:
        valid: Records that passed validation.
        invalid: Tuples of ``(record, error_message)`` for records that failed.
    """

    valid: list[dict[str, Any]] = field(default_factory=list)
    invalid: list[tuple[dict[str, Any], str]] = field(default_factory=list)

    @property
    def total(self) -> int:
        """Total number of records processed."""
        return len(self.valid) + len(self.invalid)

    @property
    def pass_rate(self) -> float:
        """Fraction of records that passed validation (0.0 -- 1.0)."""
        if self.total == 0:
            return 1.0
        return len(self.valid) / self.total

    def merge(self, other: ValidationResult) -> ValidationResult:
        """Combine two results into a new :class:`ValidationResult`."""
        return ValidationResult(
            valid=self.valid + other.valid,
            invalid=self.invalid + other.invalid,
        )


class SchemaValidator:
    """Validate records against a Pydantic model schema.

    Parameters
    ----------
    schema:
        A :class:`pydantic.BaseModel` subclass describing the expected
        record structure.

    Example::

        from pydantic import BaseModel

        class UserRecord(BaseModel):
            name: str
            age: int
            email: str

        validator = SchemaValidator(UserRecord)
        result = validator.validate([
            {"name": "Alice", "age": 30, "email": "alice@example.com"},
            {"name": "Bob", "age": "not-a-number", "email": "bob@example.com"},
        ])
        assert len(result.valid) == 1
        assert len(result.invalid) == 1
    """

    def __init__(self, schema: type[BaseModel]) -> None:
        if not (isinstance(schema, type) and issubclass(schema, BaseModel)):
            raise TypeError(
                f"schema must be a Pydantic BaseModel subclass, got {schema!r}"
            )
        self._schema = schema

    @property
    def schema(self) -> type[BaseModel]:
        """The Pydantic model class used for validation."""
        return self._schema

    def validate(self, records: list[dict[str, Any]]) -> ValidationResult:
        """Validate each record against the schema.

        Records that conform to the schema are placed in
        :attr:`ValidationResult.valid`; those that do not are placed in
        :attr:`ValidationResult.invalid` with a human-readable error message.

        Args:
            records: List of record dicts to validate.

        Returns:
            A :class:`ValidationResult` with partitioned records.
        """
        result = ValidationResult()

        for record in records:
            try:
                validated = self._schema.model_validate(record)
                # Re-serialize to dict so downstream code always works with
                # plain dicts (and benefits from any Pydantic coercion).
                result.valid.append(validated.model_dump())
            except ValidationError as exc:
                error_messages: list[str] = []
                for error in exc.errors():
                    loc = " -> ".join(str(part) for part in error["loc"])
                    error_messages.append(f"{loc}: {error['msg']}")
                result.invalid.append((record, "; ".join(error_messages)))

        return result

    def validate_one(self, record: dict[str, Any]) -> tuple[bool, str]:
        """Validate a single record.

        Returns:
            A tuple of ``(is_valid, error_message)``.  *error_message* is
            empty when the record is valid.
        """
        try:
            self._schema.model_validate(record)
            return True, ""
        except ValidationError as exc:
            parts: list[str] = []
            for error in exc.errors():
                loc = " -> ".join(str(part) for part in error["loc"])
                parts.append(f"{loc}: {error['msg']}")
            return False, "; ".join(parts)

    def __repr__(self) -> str:
        return f"SchemaValidator(schema={self._schema.__name__})"
