"""Tests for pipeline_engine.validation.schema — Pydantic-based schema validation."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from pipeline_engine.validation.schema import SchemaValidator, ValidationResult

# ---------------------------------------------------------------------------
# Test schema
# ---------------------------------------------------------------------------


class UserRecord(BaseModel):
    name: str
    age: int
    email: str


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSchemaValidator:
    def test_valid_records_pass(self):
        """All records conforming to the schema end up in the valid list."""
        validator = SchemaValidator(UserRecord)
        records = [
            {"name": "Alice", "age": 30, "email": "alice@example.com"},
            {"name": "Bob", "age": 25, "email": "bob@example.com"},
        ]
        result = validator.validate(records)

        assert len(result.valid) == 2
        assert len(result.invalid) == 0
        assert result.pass_rate == 1.0

    def test_invalid_records_caught(self):
        """Records that violate the schema are placed in the invalid list."""
        validator = SchemaValidator(UserRecord)
        records = [
            {"name": "Alice", "age": "not_a_number", "email": "alice@example.com"},
            {"name": "Bob"},  # missing required fields
        ]
        result = validator.validate(records)

        assert len(result.valid) == 0
        assert len(result.invalid) == 2
        # Each invalid entry is a (record, error_message) tuple
        for record, error_msg in result.invalid:
            assert isinstance(error_msg, str)
            assert len(error_msg) > 0

    def test_mixed_records_partitioned(self):
        """A mix of valid and invalid records is partitioned correctly."""
        validator = SchemaValidator(UserRecord)
        records = [
            {"name": "Alice", "age": 30, "email": "alice@example.com"},
            {"name": "Bad", "age": "xyz", "email": "bad@example.com"},
            {"name": "Charlie", "age": 35, "email": "charlie@example.com"},
        ]
        result = validator.validate(records)

        assert len(result.valid) == 2
        assert len(result.invalid) == 1
        assert result.total == 3
        assert 0.6 < result.pass_rate < 0.7  # 2/3

    def test_validate_one(self):
        """validate_one returns (True, '') for valid and (False, msg) for invalid."""
        validator = SchemaValidator(UserRecord)

        is_valid, msg = validator.validate_one(
            {"name": "Alice", "age": 30, "email": "a@b.com"}
        )
        assert is_valid is True
        assert msg == ""

        is_valid, msg = validator.validate_one({"name": "Bad"})
        assert is_valid is False
        assert "age" in msg

    def test_merge_results(self):
        """Two ValidationResult objects can be merged."""
        r1 = ValidationResult(
            valid=[{"a": 1}],
            invalid=[({"b": 2}, "error1")],
        )
        r2 = ValidationResult(
            valid=[{"c": 3}, {"d": 4}],
            invalid=[],
        )
        merged = r1.merge(r2)

        assert len(merged.valid) == 3
        assert len(merged.invalid) == 1
        assert merged.total == 4

    def test_empty_records_returns_empty_result(self):
        """Validating an empty list returns an empty result with pass_rate 1.0."""
        validator = SchemaValidator(UserRecord)
        result = validator.validate([])

        assert result.total == 0
        assert result.pass_rate == 1.0

    def test_non_model_schema_raises(self):
        """Passing a non-BaseModel type raises TypeError."""
        with pytest.raises(TypeError, match="Pydantic BaseModel"):
            SchemaValidator(dict)
