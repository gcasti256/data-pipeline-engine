"""Kafka source and sink connectors for the pipeline DAG."""

from __future__ import annotations

import ast
import asyncio
import json
import logging
import operator
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

from pipeline_engine.connectors.base import BaseConnector, ConnectorConfig

logger = logging.getLogger(__name__)

# Operators allowed in filter condition expressions.
_COMPARE_OPS: dict[type, Any] = {
    ast.Gt: operator.gt,
    ast.Lt: operator.lt,
    ast.GtE: operator.ge,
    ast.LtE: operator.le,
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
}


def _safe_eval_node(node: ast.AST, record: dict[str, Any]) -> Any:
    """Evaluate an AST node against a record dict.

    Only supports comparisons, boolean logic, field name lookups, and
    literal values — no function calls, attribute access, or subscripts.
    """
    if isinstance(node, ast.Compare):
        left = _safe_eval_node(node.left, record)
        for op, comparator in zip(node.ops, node.comparators):
            right = _safe_eval_node(comparator, record)
            op_func = _COMPARE_OPS.get(type(op))
            if op_func is None:
                raise ValueError(f"Unsupported operator: {type(op).__name__}")
            if not op_func(left, right):
                return False
        return True
    if isinstance(node, ast.BoolOp):
        if isinstance(node.op, ast.And):
            return all(_safe_eval_node(v, record) for v in node.values)
        if isinstance(node.op, ast.Or):
            return any(_safe_eval_node(v, record) for v in node.values)
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
        return not _safe_eval_node(node.operand, record)
    if isinstance(node, ast.Name):
        return record.get(node.id)
    if isinstance(node, ast.Constant):
        return node.value
    raise ValueError(f"Unsupported expression node: {type(node).__name__}")


@dataclass
class KafkaConnectorConfig(ConnectorConfig):
    """Extended configuration for Kafka connectors."""

    brokers: list[str] = field(default_factory=lambda: ["localhost:9092"])
    topic: str = ""
    consumer_group: str = "pipeline-engine"
    format: str = "json"  # json, avro, raw
    auto_offset_reset: str = "earliest"
    schema_registry_url: str | None = None
    avro_schema: dict[str, Any] | None = None
    poll_timeout_seconds: float = 5.0
    max_messages: int | None = None
    dead_letter_topic: str | None = None
    idempotent: bool = True


class KafkaSource(BaseConnector):
    """Kafka source connector that reads from a topic and yields records.

    Reads messages from a Kafka topic, deserializes them, and feeds them
    into the pipeline as records. Supports JSON and Avro deserialization.

    Parameters
    ----------
    brokers:
        List of Kafka broker addresses.
    topic:
        Topic to consume from.
    consumer_group:
        Consumer group ID.
    format:
        Deserialization format (``json``, ``avro``, ``raw``).
    auto_offset_reset:
        Where to start reading (``earliest`` or ``latest``).
    config:
        Additional connector configuration.
    max_messages:
        Maximum number of messages to consume (``None`` for unlimited).

    Example::

        source = KafkaSource(
            brokers=["localhost:9092"],
            topic="raw-transactions",
            consumer_group="etl-pipeline",
        )
        records = await source.read()
    """

    def __init__(
        self,
        brokers: list[str] | None = None,
        topic: str = "",
        consumer_group: str = "pipeline-engine",
        format: str = "json",
        auto_offset_reset: str = "earliest",
        config: ConnectorConfig | None = None,
        max_messages: int | None = None,
        schema_registry_url: str | None = None,
        avro_schema: dict[str, Any] | None = None,
        poll_timeout_seconds: float = 5.0,
    ) -> None:
        super().__init__(config)
        self._brokers = brokers or ["localhost:9092"]
        self._topic = topic
        self._consumer_group = consumer_group
        self._format = format
        self._auto_offset_reset = auto_offset_reset
        self._max_messages = max_messages
        self._schema_registry_url = schema_registry_url
        self._avro_schema = avro_schema
        self._poll_timeout = poll_timeout_seconds
        self._consumer: Any = None

    def _create_consumer(self) -> Any:
        try:
            from confluent_kafka import Consumer
        except ImportError:
            raise ImportError(
                "confluent-kafka is required for Kafka connectors. "
                "Install with: pip install pipeline-engine[kafka]"
            )

        conf = {
            "bootstrap.servers": ",".join(self._brokers),
            "group.id": self._consumer_group,
            "auto.offset.reset": self._auto_offset_reset,
            "enable.auto.commit": True,
        }
        consumer = Consumer(conf)
        consumer.subscribe([self._topic])
        return consumer

    def _deserialize(self, raw: bytes) -> dict[str, Any]:
        if self._format == "json":
            return json.loads(raw)
        elif self._format == "avro":
            try:
                import fastavro
                import io

                reader = fastavro.reader(io.BytesIO(raw))
                records = list(reader)
                return records[0] if records else {}
            except ImportError:
                raise ImportError(
                    "fastavro is required for Avro deserialization. "
                    "Install with: pip install pipeline-engine[kafka]"
                )
        elif self._format == "raw":
            return {"raw": raw.decode("utf-8", errors="replace")}
        raise ValueError(f"Unknown format: {self._format}")

    async def read(self, **kwargs: Any) -> list[dict[str, Any]]:
        """Read all available messages from the topic.

        Consumes messages up to ``max_messages`` (if set) or until no more
        messages are available within the poll timeout.
        """
        consumer = self._create_consumer()
        records: list[dict[str, Any]] = []

        try:
            empty_polls = 0
            max_empty = 3

            while True:
                if self._max_messages and len(records) >= self._max_messages:
                    break

                msg = await asyncio.to_thread(
                    consumer.poll, timeout=self._poll_timeout
                )

                if msg is None:
                    empty_polls += 1
                    if empty_polls >= max_empty:
                        break
                    continue

                if msg.error():
                    logger.warning("Kafka consumer error: %s", msg.error())
                    continue

                empty_polls = 0
                raw = msg.value()
                if raw is None:
                    continue

                try:
                    record = self._deserialize(raw)
                    record["_kafka_meta"] = {
                        "topic": msg.topic(),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                    }
                    records.append(record)
                except Exception as e:
                    logger.error("Failed to deserialize message: %s", e)
        finally:
            consumer.close()

        return records

    async def write(self, records: list[dict[str, Any]], **kwargs: Any) -> int:
        """Not supported for a source connector."""
        raise NotImplementedError("KafkaSource is read-only")

    async def read_stream(self, **kwargs: Any) -> AsyncIterator[list[dict[str, Any]]]:
        """Stream messages in batches from the Kafka topic."""
        consumer = self._create_consumer()
        total_read = 0

        try:
            while True:
                if self._max_messages and total_read >= self._max_messages:
                    break

                batch: list[dict[str, Any]] = []
                messages = await asyncio.to_thread(
                    consumer.consume,
                    num_messages=self.config.batch_size,
                    timeout=self._poll_timeout,
                )

                if not messages:
                    break

                for msg in messages:
                    if msg.error():
                        continue
                    raw = msg.value()
                    if raw is None:
                        continue

                    try:
                        record = self._deserialize(raw)
                        record["_kafka_meta"] = {
                            "topic": msg.topic(),
                            "partition": msg.partition(),
                            "offset": msg.offset(),
                        }
                        batch.append(record)
                    except Exception as e:
                        logger.error("Deserialization error: %s", e)

                if batch:
                    total_read += len(batch)
                    yield batch
        finally:
            consumer.close()

    async def close(self) -> None:
        if self._consumer:
            self._consumer.close()
            self._consumer = None

    def __repr__(self) -> str:
        return (
            f"KafkaSource(topic={self._topic!r}, "
            f"group={self._consumer_group!r}, "
            f"format={self._format!r})"
        )


class KafkaSink(BaseConnector):
    """Kafka sink connector that writes pipeline output to a topic.

    Serializes records and publishes them to a Kafka topic with delivery
    confirmation. Supports JSON and Avro serialization formats.

    Parameters
    ----------
    brokers:
        List of Kafka broker addresses.
    topic:
        Topic to produce to.
    format:
        Serialization format (``json``, ``avro``).
    key_field:
        Optional field to use as the message key for partitioning.
    config:
        Additional connector configuration.
    condition:
        Optional filter expression — only records matching the condition
        are published (used for conditional routing like alerts).

    Example::

        sink = KafkaSink(
            brokers=["localhost:9092"],
            topic="enriched-transactions",
            format="json",
        )
        written = await sink.write(records)
    """

    def __init__(
        self,
        brokers: list[str] | None = None,
        topic: str = "",
        format: str = "json",
        key_field: str | None = None,
        config: ConnectorConfig | None = None,
        idempotent: bool = True,
        avro_schema: dict[str, Any] | None = None,
        condition: str | None = None,
    ) -> None:
        super().__init__(config)
        self._brokers = brokers or ["localhost:9092"]
        self._topic = topic
        self._format = format
        self._key_field = key_field
        self._idempotent = idempotent
        self._avro_schema = avro_schema
        self._condition = condition
        self._producer: Any = None
        self._delivery_count = 0
        self._error_count = 0

    def _create_producer(self) -> Any:
        try:
            from confluent_kafka import Producer
        except ImportError:
            raise ImportError(
                "confluent-kafka is required for Kafka connectors. "
                "Install with: pip install pipeline-engine[kafka]"
            )

        conf: dict[str, Any] = {
            "bootstrap.servers": ",".join(self._brokers),
            "acks": "all",
        }
        if self._idempotent:
            conf["enable.idempotence"] = True

        return Producer(conf)

    def _serialize(self, record: dict[str, Any]) -> bytes:
        # Strip internal metadata before serializing
        clean = {k: v for k, v in record.items() if not k.startswith("_")}

        if self._format == "json":
            return json.dumps(clean, default=str).encode("utf-8")
        elif self._format == "avro":
            try:
                import fastavro
                import io

                if not self._avro_schema:
                    raise ValueError("Avro schema required for Avro serialization")
                buf = io.BytesIO()
                fastavro.schemaless_writer(buf, self._avro_schema, clean)
                return buf.getvalue()
            except ImportError:
                raise ImportError(
                    "fastavro is required for Avro serialization. "
                    "Install with: pip install pipeline-engine[kafka]"
                )
        raise ValueError(f"Unknown serialization format: {self._format}")

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        if err:
            self._error_count += 1
            logger.error("Delivery failed: %s", err)
        else:
            self._delivery_count += 1

    def _evaluate_condition(self, record: dict[str, Any]) -> bool:
        """Evaluate an optional filter condition on a record.

        Uses AST-based safe evaluation — only comparisons, boolean ops,
        field lookups, and literal values are permitted.
        """
        if not self._condition:
            return True

        try:
            tree = ast.parse(self._condition, mode="eval")
            return bool(_safe_eval_node(tree.body, record))
        except Exception:
            return False

    async def read(self, **kwargs: Any) -> list[dict[str, Any]]:
        """Not supported for a sink connector."""
        raise NotImplementedError("KafkaSink is write-only")

    async def write(self, records: list[dict[str, Any]], **kwargs: Any) -> int:
        """Write records to the Kafka topic.

        Returns the number of records successfully queued for delivery.
        """
        if not self._producer:
            self._producer = self._create_producer()

        sent = 0
        for record in records:
            if not self._evaluate_condition(record):
                continue

            serialized = self._serialize(record)

            produce_kwargs: dict[str, Any] = {
                "topic": self._topic,
                "value": serialized,
                "callback": self._delivery_callback,
            }

            if self._key_field and self._key_field in record:
                produce_kwargs["key"] = str(record[self._key_field]).encode("utf-8")

            await asyncio.to_thread(self._producer.produce, **produce_kwargs)
            sent += 1

        # Trigger delivery callbacks
        self._producer.poll(0)

        return sent

    async def close(self) -> None:
        if self._producer:
            await asyncio.to_thread(self._producer.flush, timeout=30)
            self._producer = None

    def __repr__(self) -> str:
        return (
            f"KafkaSink(topic={self._topic!r}, "
            f"format={self._format!r})"
        )
