"""Parse YAML pipeline configurations and build executable DAGs."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml

from pipeline_engine.config.models import (
    PipelineConfig,
    SinkConfig,
    SourceConfig,
    TransformConfig,
)
from pipeline_engine.connectors.base import BaseConnector, ConnectorConfig
from pipeline_engine.connectors.csv_connector import CSVConnector
from pipeline_engine.connectors.json_connector import JSONConnector
from pipeline_engine.connectors.sqlite_connector import SQLiteConnector
from pipeline_engine.core.dag import DAG, Node
from pipeline_engine.transforms.aggregate import AggregateTransform
from pipeline_engine.transforms.base import BaseTransform
from pipeline_engine.transforms.deduplicate import DeduplicateTransform
from pipeline_engine.transforms.filter import FilterTransform
from pipeline_engine.transforms.join import JoinTransform
from pipeline_engine.transforms.map import MapTransform
from pipeline_engine.transforms.window import WindowTransform

logger = logging.getLogger(__name__)

# Registry of known connector types.
_CONNECTOR_TYPES: dict[str, type] = {
    "csv": CSVConnector,
    "json": JSONConnector,
    "sqlite": SQLiteConnector,
}

# Registry of known transform types.
_TRANSFORM_TYPES: dict[str, str] = {
    "filter": "filter",
    "map": "map",
    "aggregate": "aggregate",
    "join": "join",
    "deduplicate": "deduplicate",
    "window": "window",
    "schema_validation": "schema_validation",
    "window_aggregate": "window_aggregate",
}

# Try to register optional connectors.
try:
    from pipeline_engine.connectors.postgres_connector import PostgresConnector
    _CONNECTOR_TYPES["postgres"] = PostgresConnector
except ImportError:  # pragma: no cover — optional dependency
    pass

try:
    from pipeline_engine.connectors.rest_connector import RESTConnector
    _CONNECTOR_TYPES["rest"] = RESTConnector
except ImportError:  # pragma: no cover — optional dependency
    pass

try:
    from pipeline_engine.connectors.kafka_connector import KafkaSource, KafkaSink
    _CONNECTOR_TYPES["kafka"] = KafkaSource  # source default; sink uses KafkaSink
except ImportError:  # pragma: no cover — optional dependency
    pass


def parse_config(path: str) -> PipelineConfig:
    """Read a YAML file and return a validated :class:`PipelineConfig`.

    Args:
        path: Path to the YAML configuration file.

    Returns:
        A :class:`PipelineConfig` instance.

    Raises:
        FileNotFoundError: If the YAML file does not exist.
        yaml.YAMLError: If the file is not valid YAML.
        pydantic.ValidationError: If the YAML structure does not match
            the expected schema.
    """
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with config_path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise ValueError(
            f"Expected a YAML mapping at top level, got {type(raw).__name__}"
        )

    return PipelineConfig.model_validate(raw)


def validate_config(config: PipelineConfig) -> list[str]:
    """Check a :class:`PipelineConfig` for semantic issues.

    Performs checks that go beyond Pydantic schema validation, such as
    unknown transform types, dangling input references, and missing
    required fields for specific transform types.

    Args:
        config: The parsed pipeline configuration.

    Returns:
        A list of warning/error strings.  An empty list means the
        configuration looks clean.
    """
    warnings: list[str] = []

    # Collect all known node names (sources + transforms).
    known_names: set[str] = set(config.sources.keys())
    for tc in config.transforms:
        if tc.name in known_names:
            warnings.append(
                f"Duplicate name '{tc.name}' — transform name collides with "
                f"an existing source or transform"
            )
        known_names.add(tc.name)

    # Check source connector types.
    all_known_types = set(_CONNECTOR_TYPES) | {"kafka"}
    for name, sc in config.sources.items():
        if sc.type not in all_known_types:
            warnings.append(
                f"Source '{name}': unknown connector type '{sc.type}'"
            )

    # Check transforms.
    for tc in config.transforms:
        if tc.type not in _TRANSFORM_TYPES:
            warnings.append(
                f"Transform '{tc.name}': unknown type '{tc.type}'"
            )

        if tc.input and tc.input not in known_names:
            warnings.append(
                f"Transform '{tc.name}': input '{tc.input}' does not "
                f"reference a known source or transform"
            )

        # Type-specific field checks.
        if tc.type == "filter" and not tc.condition:
            warnings.append(
                f"Transform '{tc.name}': 'filter' type requires a 'condition'"
            )
        if tc.type == "map" and not tc.columns:
            warnings.append(
                f"Transform '{tc.name}': 'map' type requires 'columns'"
            )
        if tc.type == "aggregate":
            if not tc.group_by:
                warnings.append(
                    f"Transform '{tc.name}': 'aggregate' type requires 'group_by'"
                )
            if not tc.aggregations:
                warnings.append(
                    f"Transform '{tc.name}': 'aggregate' type requires 'aggregations'"
                )
        if tc.type == "deduplicate" and not tc.keys:
            warnings.append(
                f"Transform '{tc.name}': 'deduplicate' type requires 'keys'"
            )
        if tc.type == "window":
            if tc.size is None:
                warnings.append(
                    f"Transform '{tc.name}': 'window' type requires 'size'"
                )

    # Check sink connector types and input references.
    for name, sink in config.sinks.items():
        if sink.type not in all_known_types:
            warnings.append(
                f"Sink '{name}': unknown connector type '{sink.type}'"
            )
        if sink.input and sink.input not in known_names:
            warnings.append(
                f"Sink '{name}': input '{sink.input}' does not reference "
                f"a known source or transform"
            )

    return warnings


def build_dag(
    config: PipelineConfig,
) -> tuple[DAG, dict[str, Any]]:
    """Construct an executable DAG from a pipeline configuration.

    Creates connector and transform instances and wires them into a DAG.
    Returns the DAG together with a context dict that maps node IDs to
    their operation objects (connectors / transforms).

    Args:
        config: The parsed and validated pipeline configuration.

    Returns:
        A tuple of ``(dag, context)`` where *context* maps each node ID
        to a dict containing the connector or transform instance and its
        role (``"source"``, ``"transform"``, or ``"sink"``).

    Raises:
        ValueError: If required connectors or transforms cannot be created.
    """
    dag = DAG(name=config.name)
    context: dict[str, Any] = {}

    for name, sc in config.sources.items():
        connector = _build_connector(sc, role="source")
        operation = _make_source_operation(connector)
        node = Node(id=name, operation=operation)
        dag.add_node(node)
        context[name] = {"instance": connector, "role": "source"}

    for tc in config.transforms:
        transform = _build_transform(tc)
        operation = _make_transform_operation(transform)
        node = Node(id=tc.name, operation=operation)
        dag.add_node(node)
        context[tc.name] = {"instance": transform, "role": "transform"}

        # Wire dependency edge.
        if tc.input:
            dag.add_edge(tc.input, tc.name)

    for name, sink_cfg in config.sinks.items():
        connector = _build_connector(sink_cfg, role="sink")
        operation = _make_sink_operation(connector, sink_cfg)
        node = Node(id=name, operation=operation)
        dag.add_node(node)
        context[name] = {"instance": connector, "role": "sink"}

        # Wire dependency edge.
        if sink_cfg.input:
            dag.add_edge(sink_cfg.input, name)

    return dag, context


def _build_connector(
    cfg: SourceConfig | SinkConfig,
    role: str,
) -> BaseConnector:
    """Instantiate a connector from its configuration."""
    connector_cls = _CONNECTOR_TYPES.get(cfg.type)
    if connector_cls is None:
        raise ValueError(
            f"Unknown {role} connector type: '{cfg.type}'.  "
            f"Available: {', '.join(sorted(_CONNECTOR_TYPES))}"
        )

    conn_config = ConnectorConfig(
        batch_size=cfg.extra.get("batch_size", 1000),
        extra=cfg.extra,
    )

    if cfg.type == "csv":
        if not cfg.path:
            raise ValueError(f"CSV {role} requires a 'path'")
        return CSVConnector(path=cfg.path, config=conn_config)

    if cfg.type == "json":
        if not cfg.path:
            raise ValueError(f"JSON {role} requires a 'path'")
        return JSONConnector(path=cfg.path, config=conn_config)

    if cfg.type == "sqlite":
        if not cfg.database:
            raise ValueError(f"SQLite {role} requires a 'database'")
        return SQLiteConnector(database=cfg.database, config=conn_config)

    if cfg.type == "postgres":
        if not cfg.database:
            raise ValueError(f"Postgres {role} requires a 'database'")
        from pipeline_engine.connectors.postgres_connector import PostgresConnector
        return PostgresConnector(dsn=cfg.database, config=conn_config)

    if cfg.type == "rest":
        if not cfg.url:
            raise ValueError(f"REST {role} requires a 'url'")
        from pipeline_engine.connectors.rest_connector import RESTConnector
        return RESTConnector(base_url=cfg.url, config=conn_config)

    if cfg.type == "kafka":
        kafka_cfg = getattr(cfg, "config", None)
        if not kafka_cfg:
            raise ValueError(f"Kafka {role} requires a 'config' block with topic and brokers")
        from pipeline_engine.connectors.kafka_connector import KafkaSource, KafkaSink
        if role == "source":
            return KafkaSource(
                brokers=kafka_cfg.brokers,
                topic=kafka_cfg.topic,
                consumer_group=kafka_cfg.consumer_group,
                format=kafka_cfg.format,
                config=conn_config,
            )
        else:
            return KafkaSink(
                brokers=kafka_cfg.brokers,
                topic=kafka_cfg.topic,
                format=kafka_cfg.format,
                key_field=kafka_cfg.key_field,
                condition=kafka_cfg.condition,
                config=conn_config,
            )

    raise ValueError(f"Unhandled connector type: '{cfg.type}'")


def _build_transform(tc: TransformConfig) -> BaseTransform:
    """Instantiate a transform from its configuration."""
    t_type = tc.type

    if t_type == "filter":
        if not tc.condition:
            raise ValueError(f"Transform '{tc.name}': filter requires 'condition'")
        return FilterTransform(condition=tc.condition, name=tc.name)

    if t_type == "map":
        if not tc.columns:
            raise ValueError(f"Transform '{tc.name}': map requires 'columns'")
        return MapTransform(columns=tc.columns, name=tc.name)

    if t_type == "aggregate":
        if not tc.group_by or not tc.aggregations:
            raise ValueError(
                f"Transform '{tc.name}': aggregate requires 'group_by' and 'aggregations'"
            )
        return AggregateTransform(
            group_by=tc.group_by,
            aggregations=tc.aggregations,
            name=tc.name,
        )

    if t_type == "join":
        if not tc.on:
            raise ValueError(f"Transform '{tc.name}': join requires 'on'")
        return JoinTransform(
            right_data=[],  # Right data resolved at runtime from upstream
            on=tc.on,
            how=tc.how or "inner",
            name=tc.name,
        )

    if t_type == "deduplicate":
        if not tc.keys:
            raise ValueError(f"Transform '{tc.name}': deduplicate requires 'keys'")
        return DeduplicateTransform(
            keys=tc.keys,
            keep=tc.keep or "first",
            name=tc.name,
        )

    if t_type == "window":
        if tc.size is None:
            raise ValueError(f"Transform '{tc.name}': window requires 'size'")
        return WindowTransform(
            size=tc.size,
            step=tc.step or 1,
            aggregation=tc.aggregation or "avg",
            column=tc.column or "",
            name=tc.name,
        )

    raise ValueError(f"Unknown transform type: '{t_type}'")


def _make_source_operation(connector: BaseConnector) -> Any:
    """Create an async operation callable for a source connector node."""

    async def _read(ctx: dict[str, Any]) -> list[dict[str, Any]]:
        return await connector.read()

    return _read


def _make_transform_operation(transform: BaseTransform) -> Any:
    """Create an operation callable for a transform node."""

    def _transform(ctx: dict[str, Any]) -> list[dict[str, Any]]:
        # Collect upstream data from results.
        results = ctx.get("results", {})
        input_data: list[dict[str, Any]] = []
        for dep_output in results.values():
            if isinstance(dep_output, list):
                input_data.extend(dep_output)
        return transform.execute(input_data)

    return _transform


def _make_sink_operation(
    connector: BaseConnector,
    cfg: SinkConfig,
) -> Any:
    """Create an async operation callable for a sink connector node."""

    async def _write(ctx: dict[str, Any]) -> int:
        results = ctx.get("results", {})
        records: list[dict[str, Any]] = []
        for dep_output in results.values():
            if isinstance(dep_output, list):
                records.extend(dep_output)

        kwargs: dict[str, Any] = {}
        if cfg.table:
            kwargs["table"] = cfg.table
        return await connector.write(records, **kwargs)

    return _write
