"""Kafka producer for pipeline output sinks with async batched publishing."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

logger = logging.getLogger(__name__)


class SerializationFormat(str, Enum):
    JSON = "json"
    AVRO = "avro"
    RAW = "raw"


@dataclass
class KafkaProducerConfig:
    """Configuration for the Kafka producer."""

    brokers: list[str] = field(default_factory=lambda: ["localhost:9092"])
    topic: str = ""
    format: SerializationFormat = SerializationFormat.JSON
    batch_size: int = 100
    linger_ms: int = 5
    acks: str = "all"
    retries: int = 3
    retry_backoff_ms: int = 100
    max_in_flight: int = 5
    idempotent: bool = True
    compression_type: str = "none"
    schema_registry_url: str | None = None
    avro_schema: dict[str, Any] | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class ProducerMetrics:
    """Metrics for tracking producer performance."""

    messages_sent: int = 0
    messages_failed: int = 0
    bytes_sent: int = 0
    delivery_confirmed: int = 0
    last_send_time: float | None = None
    started_at: float = field(default_factory=time.monotonic)

    @property
    def uptime_seconds(self) -> float:
        return time.monotonic() - self.started_at

    def to_dict(self) -> dict[str, Any]:
        return {
            "messages_sent": self.messages_sent,
            "messages_failed": self.messages_failed,
            "bytes_sent": self.bytes_sent,
            "delivery_confirmed": self.delivery_confirmed,
            "uptime_seconds": round(self.uptime_seconds, 2),
        }


class KafkaProducer:
    """Kafka producer with async batched publishing and delivery confirmation.

    Supports JSON and Avro serialization, idempotent writes for exactly-once
    semantics, retry with exponential backoff, and delivery confirmation
    callbacks.

    Parameters
    ----------
    config:
        Producer configuration including brokers, topic, and format.
    on_delivery:
        Optional callback invoked on delivery confirmation.

    Example::

        config = KafkaProducerConfig(
            brokers=["localhost:9092"],
            topic="enriched-transactions",
            format=SerializationFormat.JSON,
            idempotent=True,
        )

        async with KafkaProducer(config) as producer:
            await producer.send({"amount": 100, "currency": "USD"})
            await producer.send_batch([
                {"amount": 50, "currency": "EUR"},
                {"amount": 75, "currency": "GBP"},
            ])
    """

    def __init__(
        self,
        config: KafkaProducerConfig,
        on_delivery: Callable[[Any, Any], None] | None = None,
    ) -> None:
        self._config = config
        self._on_delivery = on_delivery
        self._producer: Any = None
        self._metrics = ProducerMetrics()

    @property
    def config(self) -> KafkaProducerConfig:
        return self._config

    @property
    def metrics(self) -> ProducerMetrics:
        return self._metrics

    def _create_producer(self) -> Any:
        """Create the underlying confluent-kafka Producer instance."""
        try:
            from confluent_kafka import Producer
        except ImportError:
            raise ImportError(
                "confluent-kafka is required for Kafka support. "
                "Install with: pip install pipeline-engine[kafka]"
            )

        conf: dict[str, Any] = {
            "bootstrap.servers": ",".join(self._config.brokers),
            "linger.ms": self._config.linger_ms,
            "acks": self._config.acks,
            "retries": self._config.retries,
            "retry.backoff.ms": self._config.retry_backoff_ms,
            "max.in.flight.requests.per.connection": self._config.max_in_flight,
            "compression.type": self._config.compression_type,
        }

        if self._config.idempotent:
            conf["enable.idempotence"] = True

        conf.update(self._config.extra)
        return Producer(conf)

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        """Internal delivery report callback."""
        if err is not None:
            self._metrics.messages_failed += 1
            logger.error("Delivery failed for %s: %s", msg.topic(), err)
        else:
            self._metrics.delivery_confirmed += 1

        if self._on_delivery:
            self._on_delivery(err, msg)

    def _serialize(self, record: dict[str, Any]) -> bytes:
        """Serialize a record according to the configured format."""
        if self._config.format == SerializationFormat.JSON:
            return json.dumps(record, default=str).encode("utf-8")
        elif self._config.format == SerializationFormat.AVRO:
            try:
                import fastavro
                import io

                if not self._config.avro_schema:
                    raise ValueError("Avro schema required for Avro serialization")

                buf = io.BytesIO()
                fastavro.schemaless_writer(buf, self._config.avro_schema, record)
                return buf.getvalue()
            except ImportError:
                raise ImportError(
                    "fastavro is required for Avro serialization. "
                    "Install with: pip install pipeline-engine[kafka]"
                )
        elif self._config.format == SerializationFormat.RAW:
            return str(record).encode("utf-8")
        else:
            raise ValueError(f"Unknown format: {self._config.format}")

    async def connect(self) -> None:
        """Initialize the producer connection."""
        self._producer = self._create_producer()
        self._metrics = ProducerMetrics()
        logger.info(
            "KafkaProducer connected: brokers=%s, topic=%s",
            self._config.brokers,
            self._config.topic,
        )

    async def send(
        self,
        record: dict[str, Any],
        topic: str | None = None,
        key: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Send a single record to Kafka.

        Parameters
        ----------
        record:
            The record to serialize and send.
        topic:
            Override the default topic for this message.
        key:
            Optional message key for partitioning.
        headers:
            Optional message headers.
        """
        if not self._producer:
            await self.connect()

        target_topic = topic or self._config.topic
        if not target_topic:
            raise ValueError("No topic specified")

        serialized = self._serialize(record)

        kwargs: dict[str, Any] = {
            "topic": target_topic,
            "value": serialized,
            "callback": self._delivery_callback,
        }
        if key:
            kwargs["key"] = key.encode("utf-8")
        if headers:
            kwargs["headers"] = [(k, v.encode("utf-8")) for k, v in headers.items()]

        await asyncio.to_thread(self._producer.produce, **kwargs)

        self._metrics.messages_sent += 1
        self._metrics.bytes_sent += len(serialized)
        self._metrics.last_send_time = time.monotonic()

        # Trigger delivery callbacks
        self._producer.poll(0)

    async def send_batch(
        self,
        records: list[dict[str, Any]],
        topic: str | None = None,
        key_fn: Callable[[dict[str, Any]], str | None] | None = None,
    ) -> int:
        """Send a batch of records to Kafka.

        Parameters
        ----------
        records:
            List of records to send.
        topic:
            Override topic for all messages in the batch.
        key_fn:
            Optional function to extract a message key from each record.

        Returns
        -------
        int
            Number of records queued for sending.
        """
        if not self._producer:
            await self.connect()

        sent = 0
        for record in records:
            key = key_fn(record) if key_fn else None
            await self.send(record, topic=topic, key=key)
            sent += 1

        return sent

    async def flush(self, timeout: float = 30.0) -> None:
        """Wait for all buffered messages to be delivered.

        Parameters
        ----------
        timeout:
            Maximum seconds to wait for delivery.
        """
        if self._producer:
            remaining = await asyncio.to_thread(
                self._producer.flush, timeout=timeout
            )
            if remaining > 0:
                logger.warning(
                    "%d messages still in queue after flush timeout", remaining
                )

    async def close(self) -> None:
        """Flush and close the producer."""
        if self._producer:
            await self.flush()
            self._producer = None
        logger.info("KafkaProducer closed")

    async def __aenter__(self) -> KafkaProducer:
        await self.connect()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    def __repr__(self) -> str:
        return (
            f"KafkaProducer(topic={self._config.topic}, "
            f"format={self._config.format.value})"
        )
