"""Tests for the schema registry module."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from pipeline_engine.streaming.schema_registry import (
    CompatibilityLevel,
    SchemaRegistryClient,
    SchemaType,
    SchemaValidator,
    SchemaVersion,
    ValidationResult,
)


class TestSchemaVersion:
    def test_creation(self):
        sv = SchemaVersion(
            subject="test-value",
            version=1,
            schema_id=42,
            schema_type=SchemaType.JSON_SCHEMA,
            schema={"type": "object"},
        )
        assert sv.subject == "test-value"
        assert sv.version == 1
        assert sv.schema_id == 42

    def test_fingerprint_deterministic(self):
        schema = {"type": "object", "properties": {"id": {"type": "string"}}}
        sv1 = SchemaVersion(
            subject="t", version=1, schema_id=1,
            schema_type=SchemaType.JSON_SCHEMA, schema=schema,
        )
        sv2 = SchemaVersion(
            subject="t", version=2, schema_id=2,
            schema_type=SchemaType.JSON_SCHEMA, schema=schema,
        )
        assert sv1.fingerprint == sv2.fingerprint

    def test_fingerprint_changes_with_schema(self):
        sv1 = SchemaVersion(
            subject="t", version=1, schema_id=1,
            schema_type=SchemaType.JSON_SCHEMA,
            schema={"type": "object"},
        )
        sv2 = SchemaVersion(
            subject="t", version=1, schema_id=1,
            schema_type=SchemaType.JSON_SCHEMA,
            schema={"type": "array"},
        )
        assert sv1.fingerprint != sv2.fingerprint


class TestValidationResult:
    def test_valid(self):
        r = ValidationResult(is_valid=True)
        assert r.is_valid
        assert r.errors == []

    def test_invalid(self):
        r = ValidationResult(is_valid=False, errors=["field required"])
        assert not r.is_valid
        assert len(r.errors) == 1

    def test_to_dict(self):
        r = ValidationResult(is_valid=False, errors=["err1"])
        d = r.to_dict()
        assert d["is_valid"] is False
        assert d["errors"] == ["err1"]


class TestSchemaValidator:
    def test_json_schema_valid(self):
        schema = {
            "type": "object",
            "required": ["id", "amount"],
            "properties": {
                "id": {"type": "string"},
                "amount": {"type": "number", "minimum": 0},
            },
        }
        validator = SchemaValidator(schema, SchemaType.JSON_SCHEMA)
        result = validator.validate({"id": "tx1", "amount": 100})
        assert result.is_valid

    def test_json_schema_invalid_missing_field(self):
        schema = {
            "type": "object",
            "required": ["id", "amount"],
            "properties": {
                "id": {"type": "string"},
                "amount": {"type": "number"},
            },
        }
        validator = SchemaValidator(schema, SchemaType.JSON_SCHEMA)
        result = validator.validate({"id": "tx1"})
        assert not result.is_valid
        assert len(result.errors) > 0

    def test_json_schema_invalid_type(self):
        schema = {
            "type": "object",
            "properties": {
                "amount": {"type": "number"},
            },
        }
        validator = SchemaValidator(schema, SchemaType.JSON_SCHEMA)
        result = validator.validate({"amount": "not-a-number"})
        assert not result.is_valid

    def test_json_schema_minimum_constraint(self):
        schema = {
            "type": "object",
            "properties": {
                "amount": {"type": "number", "minimum": 0},
            },
        }
        validator = SchemaValidator(schema, SchemaType.JSON_SCHEMA)
        result = validator.validate({"amount": -5})
        assert not result.is_valid

    def test_validate_batch(self):
        schema = {
            "type": "object",
            "required": ["id"],
            "properties": {"id": {"type": "string"}},
        }
        validator = SchemaValidator(schema, SchemaType.JSON_SCHEMA)
        results = validator.validate_batch([
            {"id": "valid"},
            {"not_id": "invalid"},
            {"id": "also_valid"},
        ])
        assert len(results) == 3
        assert results[0].is_valid
        assert not results[1].is_valid
        assert results[2].is_valid

    def test_schema_properties(self):
        schema = {"type": "object"}
        validator = SchemaValidator(schema, SchemaType.JSON_SCHEMA)
        assert validator.schema == {"type": "object"}
        assert validator.schema_type == SchemaType.JSON_SCHEMA


class TestSchemaRegistryClient:
    def test_init(self):
        client = SchemaRegistryClient("http://localhost:8081")
        assert client.url == "http://localhost:8081"

    def test_init_strips_trailing_slash(self):
        client = SchemaRegistryClient("http://localhost:8081/")
        assert client.url == "http://localhost:8081"

    def test_repr(self):
        client = SchemaRegistryClient("http://localhost:8081")
        assert "localhost:8081" in repr(client)

    @pytest.mark.parametrize("schema_type,schema_type_str", [
        (SchemaType.AVRO, "AVRO"),
        (SchemaType.JSON_SCHEMA, "JSON"),
    ])
    async def test_register(self, schema_type, schema_type_str):
        client = SchemaRegistryClient("http://localhost:8081")
        schema = {"type": "record", "name": "Test", "fields": []}

        mock_http = AsyncMock()
        mock_http.is_closed = False
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"id": 1}
        mock_resp.raise_for_status = MagicMock()
        mock_http.post.return_value = mock_resp
        client._client = mock_http

        schema_id = await client.register("test-value", schema, schema_type)
        assert schema_id == 1

    async def test_get_schema_cached(self):
        client = SchemaRegistryClient("http://localhost:8081")
        cached = SchemaVersion(
            subject="test",
            version=1,
            schema_id=42,
            schema_type=SchemaType.AVRO,
            schema={"type": "string"},
        )
        client._cache[42] = cached

        result = await client.get_schema(42)
        assert result == cached

    async def test_get_versions(self):
        client = SchemaRegistryClient("http://localhost:8081")

        mock_http = AsyncMock()
        mock_http.is_closed = False
        mock_resp = MagicMock()
        mock_resp.json.return_value = [1, 2, 3]
        mock_resp.raise_for_status = MagicMock()
        mock_http.get.return_value = mock_resp
        client._client = mock_http

        versions = await client.get_versions("test-value")
        assert versions == [1, 2, 3]

    async def test_get_subjects(self):
        client = SchemaRegistryClient("http://localhost:8081")

        mock_http = AsyncMock()
        mock_http.is_closed = False
        mock_resp = MagicMock()
        mock_resp.json.return_value = ["subject1", "subject2"]
        mock_resp.raise_for_status = MagicMock()
        mock_http.get.return_value = mock_resp
        client._client = mock_http

        subjects = await client.get_subjects()
        assert subjects == ["subject1", "subject2"]

    async def test_check_compatibility(self):
        client = SchemaRegistryClient("http://localhost:8081")

        mock_http = AsyncMock()
        mock_http.is_closed = False
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"is_compatible": True}
        mock_resp.raise_for_status = MagicMock()
        mock_http.post.return_value = mock_resp
        client._client = mock_http

        result = await client.check_compatibility(
            "test-value",
            {"type": "record", "name": "Test", "fields": []},
        )
        assert result is True
