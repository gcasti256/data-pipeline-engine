"""Pydantic models for YAML-based pipeline configuration."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class KafkaConfig(BaseModel):
    """Kafka-specific configuration for source or sink connectors."""

    model_config = {"populate_by_name": True}

    brokers: list[str] = Field(
        default_factory=lambda: ["localhost:9092"],
        description="List of Kafka broker addresses",
    )
    topic: str = Field(
        ...,
        description="Kafka topic name",
    )
    consumer_group: str = Field(
        default="pipeline-engine",
        description="Consumer group ID (for source connectors)",
    )
    format: str = Field(
        default="json",
        description="Serialization format: 'json', 'avro', 'raw'",
    )
    schema_config: dict[str, Any] | None = Field(
        default=None,
        alias="schema",
        description="Schema configuration (type + path)",
    )
    condition: str | None = Field(
        default=None,
        description="Optional filter condition for conditional routing",
    )
    key_field: str | None = Field(
        default=None,
        description="Record field to use as message key for partitioning",
    )


class DeadLetterConfig(BaseModel):
    """Dead letter topic configuration for failed records."""

    type: str = Field(
        default="kafka",
        description="Dead letter destination type",
    )
    config: KafkaConfig | None = Field(
        default=None,
        description="Kafka configuration for the dead letter topic",
    )


class WindowAggregateConfig(BaseModel):
    """Configuration for windowed aggregation transforms."""

    type: str = Field(
        ...,
        description="Window type: 'tumbling', 'sliding', 'session'",
    )
    size: str = Field(
        ...,
        description="Window size (e.g. '60s', '5m', '1h')",
    )
    slide: str | None = Field(
        default=None,
        description="Slide interval for sliding windows",
    )
    gap: str | None = Field(
        default=None,
        description="Inactivity gap for session windows",
    )


class SourceConfig(BaseModel):
    """Configuration for a data source connector.

    At minimum one of *path*, *database*, *query*, *url*, or *config*
    should be set depending on the connector type.
    """

    type: str = Field(
        ...,
        description="Connector type: 'csv', 'json', 'sqlite', 'postgres', 'rest', 'kafka'",
    )
    path: str | None = Field(
        default=None,
        description="File path (for csv/json connectors)",
    )
    database: str | None = Field(
        default=None,
        description="Database path or connection string",
    )
    query: str | None = Field(
        default=None,
        description="SQL query to execute (for database connectors)",
    )
    table: str | None = Field(
        default=None,
        description="Table name (for database connectors)",
    )
    url: str | None = Field(
        default=None,
        description="URL endpoint (for REST connector)",
    )
    batch_size: int = Field(
        default=1000,
        description="Number of records per batch when streaming",
    )
    config: KafkaConfig | None = Field(
        default=None,
        description="Kafka-specific configuration (for kafka connector)",
    )
    extra: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional connector-specific settings",
    )


class TransformConfig(BaseModel):
    """Configuration for a single transform step.

    The *type* field determines which transform class is instantiated.
    Remaining fields are type-specific and optional.
    """

    model_config = {"populate_by_name": True}

    name: str = Field(
        ...,
        description="Unique name for this transform step",
    )
    type: str = Field(
        ...,
        description=(
            "Transform type: 'filter', 'map', 'aggregate', 'join', "
            "'deduplicate', 'window'"
        ),
    )
    input: str | None = Field(
        default=None,
        description="Name of the upstream source or transform that feeds this step",
    )

    # --- filter ---
    condition: str | None = Field(
        default=None,
        description="Filter condition expression (for 'filter' type)",
    )

    # --- map ---
    columns: dict[str, str] | None = Field(
        default=None,
        description="Column mapping {output: expression} (for 'map' type)",
    )

    # --- aggregate ---
    group_by: list[str] | None = Field(
        default=None,
        description="Columns to group on (for 'aggregate' type)",
    )
    aggregations: dict[str, str] | None = Field(
        default=None,
        description="Aggregation expressions {output: 'func(col)'} (for 'aggregate' type)",
    )

    # --- join ---
    on: str | list[str] | None = Field(
        default=None,
        description="Join key column(s) (for 'join' type)",
    )
    how: str | None = Field(
        default=None,
        description="Join type: 'inner', 'left', 'right', 'outer' (for 'join' type)",
    )

    # --- deduplicate ---
    keys: list[str] | None = Field(
        default=None,
        description="Key columns for deduplication (for 'deduplicate' type)",
    )
    keep: str | None = Field(
        default=None,
        description="Which duplicate to keep: 'first' or 'last' (for 'deduplicate' type)",
    )

    # --- window ---
    size: int | None = Field(
        default=None,
        description="Window size in records (for 'window' type)",
    )
    step: int | None = Field(
        default=None,
        description="Step size between windows (for 'window' type)",
    )
    column: str | None = Field(
        default=None,
        description="Source column for window aggregation (for 'window' type)",
    )
    aggregation: str | None = Field(
        default=None,
        description="Aggregation function for window (for 'window' type)",
    )

    # --- schema_validation ---
    schema_path: str | None = Field(
        default=None,
        alias="schema",
        description="Schema file path (for 'schema_validation' type)",
    )
    on_failure: str | None = Field(
        default=None,
        description="Failure handling: 'dead_letter', 'drop', 'raise' (for 'schema_validation')",
    )

    # --- window_aggregate (streaming) ---
    window: WindowAggregateConfig | None = Field(
        default=None,
        description="Window configuration for streaming aggregation",
    )


class SinkConfig(BaseModel):
    """Configuration for a data sink connector.

    Mirrors :class:`SourceConfig` with an additional *input* field to
    specify which transform output to consume.
    """

    type: str = Field(
        ...,
        description="Connector type: 'csv', 'json', 'sqlite', 'postgres', 'rest', 'kafka'",
    )
    input: str | None = Field(
        default=None,
        description="Name of the upstream transform or source that feeds this sink",
    )
    path: str | None = Field(
        default=None,
        description="File path (for csv/json connectors)",
    )
    database: str | None = Field(
        default=None,
        description="Database path or connection string",
    )
    table: str | None = Field(
        default=None,
        description="Target table name (for database connectors)",
    )
    url: str | None = Field(
        default=None,
        description="URL endpoint (for REST connector)",
    )
    config: KafkaConfig | None = Field(
        default=None,
        description="Kafka-specific configuration (for kafka connector)",
    )
    extra: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional connector-specific settings",
    )


class PipelineConfig(BaseModel):
    """Top-level pipeline configuration loaded from YAML.

    Example YAML::

        name: etl_pipeline
        version: "1.0"
        sources:
          orders_csv:
            type: csv
            path: data/orders.csv
        transforms:
          - name: filter_active
            type: filter
            input: orders_csv
            condition: "status == 'active'"
        sinks:
          output_json:
            type: json
            input: filter_active
            path: output/filtered.json
    """

    name: str = Field(
        ...,
        description="Pipeline name (used for logging and state tracking)",
    )
    version: str = Field(
        default="1.0",
        description="Configuration version string",
    )
    mode: str = Field(
        default="batch",
        description="Pipeline mode: 'batch' (default) or 'streaming'",
    )
    sources: dict[str, SourceConfig] = Field(
        ...,
        description="Named data source definitions",
    )
    transforms: list[TransformConfig] = Field(
        default_factory=list,
        description="Ordered list of transform steps",
    )
    sinks: dict[str, SinkConfig] = Field(
        ...,
        description="Named data sink definitions",
    )
    dead_letter: DeadLetterConfig | None = Field(
        default=None,
        description="Dead letter configuration for failed records",
    )
