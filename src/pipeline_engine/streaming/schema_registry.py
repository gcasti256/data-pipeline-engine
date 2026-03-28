"""Schema validation and registry client for streaming data."""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class SchemaType(str, Enum):
    JSON_SCHEMA = "json_schema"
    AVRO = "avro"


class CompatibilityLevel(str, Enum):
    NONE = "NONE"
    BACKWARD = "BACKWARD"
    FORWARD = "FORWARD"
    FULL = "FULL"
    BACKWARD_TRANSITIVE = "BACKWARD_TRANSITIVE"
    FORWARD_TRANSITIVE = "FORWARD_TRANSITIVE"
    FULL_TRANSITIVE = "FULL_TRANSITIVE"


@dataclass(frozen=True)
class SchemaVersion:
    """A versioned schema entry."""

    subject: str
    version: int
    schema_id: int
    schema_type: SchemaType
    schema: dict[str, Any]

    @property
    def fingerprint(self) -> str:
        """SHA-256 fingerprint of the canonical schema."""
        canonical = json.dumps(self.schema, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()


class SchemaValidator:
    """Validates records against JSON Schema or Avro schemas.

    Parameters
    ----------
    schema:
        The schema definition dict.
    schema_type:
        Type of schema (JSON Schema or Avro).

    Example::

        schema = {
            "type": "object",
            "required": ["id", "amount"],
            "properties": {
                "id": {"type": "string"},
                "amount": {"type": "number", "minimum": 0},
            }
        }
        validator = SchemaValidator(schema, SchemaType.JSON_SCHEMA)
        result = validator.validate({"id": "tx1", "amount": 100})
        assert result.is_valid
    """

    def __init__(
        self,
        schema: dict[str, Any],
        schema_type: SchemaType = SchemaType.JSON_SCHEMA,
    ) -> None:
        self._schema = schema
        self._schema_type = schema_type

    @property
    def schema(self) -> dict[str, Any]:
        return self._schema

    @property
    def schema_type(self) -> SchemaType:
        return self._schema_type

    def validate(self, record: dict[str, Any]) -> ValidationResult:
        """Validate a single record against the schema.

        Returns a :class:`ValidationResult` with validation status and errors.
        """
        if self._schema_type == SchemaType.JSON_SCHEMA:
            return self._validate_json_schema(record)
        elif self._schema_type == SchemaType.AVRO:
            return self._validate_avro(record)
        else:
            return ValidationResult(
                is_valid=False,
                errors=[f"Unsupported schema type: {self._schema_type}"],
            )

    def validate_batch(
        self, records: list[dict[str, Any]]
    ) -> list[ValidationResult]:
        """Validate a batch of records."""
        return [self.validate(record) for record in records]

    def _validate_json_schema(self, record: dict[str, Any]) -> ValidationResult:
        """Validate against a JSON Schema."""
        try:
            from jsonschema import validate, ValidationError as JsonSchemaError

            validate(instance=record, schema=self._schema)
            return ValidationResult(is_valid=True, errors=[])
        except JsonSchemaError as e:
            return ValidationResult(
                is_valid=False,
                errors=[e.message],
            )
        except ImportError:
            raise ImportError(
                "jsonschema is required for JSON Schema validation. "
                "Install with: pip install pipeline-engine[kafka]"
            )

    def _validate_avro(self, record: dict[str, Any]) -> ValidationResult:
        """Validate against an Avro schema."""
        try:
            import fastavro
            import io

            buf = io.BytesIO()
            try:
                fastavro.schemaless_writer(buf, self._schema, record)
                return ValidationResult(is_valid=True, errors=[])
            except Exception as e:
                return ValidationResult(
                    is_valid=False,
                    errors=[str(e)],
                )
        except ImportError:
            raise ImportError(
                "fastavro is required for Avro schema validation. "
                "Install with: pip install pipeline-engine[kafka]"
            )


@dataclass
class ValidationResult:
    """Result of a schema validation check."""

    is_valid: bool
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {"is_valid": self.is_valid, "errors": self.errors}


class SchemaRegistryClient:
    """Client for Confluent Schema Registry-compatible APIs.

    Provides schema registration, retrieval, version management, and
    compatibility checking.

    Parameters
    ----------
    url:
        Base URL of the schema registry (e.g. ``http://localhost:8081``).

    Example::

        client = SchemaRegistryClient("http://localhost:8081")
        schema_id = await client.register(
            subject="transactions-value",
            schema={"type": "record", "name": "Transaction", ...},
            schema_type=SchemaType.AVRO,
        )
        schema = await client.get_schema(schema_id)
    """

    def __init__(self, url: str) -> None:
        self._url = url.rstrip("/")
        self._cache: dict[int, SchemaVersion] = {}
        self._client: httpx.AsyncClient | None = None

    @property
    def url(self) -> str:
        return self._url

    def _get_client(self) -> httpx.AsyncClient:
        """Return a shared httpx client, creating one on first use."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient()
        return self._client

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> SchemaRegistryClient:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def register(
        self,
        subject: str,
        schema: dict[str, Any],
        schema_type: SchemaType = SchemaType.AVRO,
    ) -> int:
        """Register a new schema version under a subject.

        Returns the schema ID assigned by the registry.
        """
        body: dict[str, Any] = {
            "schema": json.dumps(schema),
        }
        if schema_type == SchemaType.JSON_SCHEMA:
            body["schemaType"] = "JSON"

        client = self._get_client()
        resp = await client.post(
            f"{self._url}/subjects/{subject}/versions",
            json=body,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        )
        resp.raise_for_status()
        return resp.json()["id"]

    async def get_schema(self, schema_id: int) -> SchemaVersion:
        """Retrieve a schema by its global ID.

        Results are cached locally for repeated lookups.
        """
        if schema_id in self._cache:
            return self._cache[schema_id]

        client = self._get_client()
        resp = await client.get(f"{self._url}/schemas/ids/{schema_id}")
        resp.raise_for_status()
        data = resp.json()

        schema_str = data["schema"]
        schema_type_str = data.get("schemaType", "AVRO")

        sv = SchemaVersion(
            subject="",
            version=0,
            schema_id=schema_id,
            schema_type=SchemaType.AVRO if schema_type_str == "AVRO" else SchemaType.JSON_SCHEMA,
            schema=json.loads(schema_str),
        )
        self._cache[schema_id] = sv
        return sv

    async def get_latest_version(self, subject: str) -> SchemaVersion:
        """Get the latest schema version for a subject."""
        client = self._get_client()
        resp = await client.get(
            f"{self._url}/subjects/{subject}/versions/latest"
        )
        resp.raise_for_status()
        data = resp.json()

        schema_type_str = data.get("schemaType", "AVRO")
        sv = SchemaVersion(
            subject=subject,
            version=data["version"],
            schema_id=data["id"],
            schema_type=SchemaType.AVRO if schema_type_str == "AVRO" else SchemaType.JSON_SCHEMA,
            schema=json.loads(data["schema"]),
        )
        self._cache[data["id"]] = sv
        return sv

    async def get_versions(self, subject: str) -> list[int]:
        """List all version numbers for a subject."""
        client = self._get_client()
        resp = await client.get(
            f"{self._url}/subjects/{subject}/versions"
        )
        resp.raise_for_status()
        return resp.json()

    async def check_compatibility(
        self,
        subject: str,
        schema: dict[str, Any],
        schema_type: SchemaType = SchemaType.AVRO,
    ) -> bool:
        """Check if a schema is compatible with the latest version.

        Returns ``True`` if compatible, ``False`` otherwise.
        """
        body: dict[str, Any] = {
            "schema": json.dumps(schema),
        }
        if schema_type == SchemaType.JSON_SCHEMA:
            body["schemaType"] = "JSON"

        client = self._get_client()
        resp = await client.post(
            f"{self._url}/compatibility/subjects/{subject}/versions/latest",
            json=body,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        )
        resp.raise_for_status()
        return resp.json().get("is_compatible", False)

    async def get_subjects(self) -> list[str]:
        """List all registered subjects."""
        client = self._get_client()
        resp = await client.get(f"{self._url}/subjects")
        resp.raise_for_status()
        return resp.json()

    async def delete_subject(self, subject: str) -> list[int]:
        """Delete a subject and all its versions.

        Returns the list of deleted version numbers.
        """
        client = self._get_client()
        resp = await client.delete(
            f"{self._url}/subjects/{subject}"
        )
        resp.raise_for_status()
        return resp.json()

    def __repr__(self) -> str:
        return f"SchemaRegistryClient(url={self._url!r})"
