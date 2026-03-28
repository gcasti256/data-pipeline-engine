"""Kafka consumer that reads from topics and feeds into pipeline nodes."""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

logger = logging.getLogger(__name__)


class DeserializationFormat(str, Enum):
    JSON = "json"
    AVRO = "avro"
    RAW = "raw"


class OffsetPolicy(str, Enum):
    AUTO = "auto"
    MANUAL = "manual"


@dataclass
class KafkaConsumerConfig:
    """Configuration for the Kafka consumer."""

    brokers: list[str] = field(default_factory=lambda: ["localhost:9092"])
    topics: list[str] = field(default_factory=list)
    consumer_group: str = "pipeline-engine"
    auto_offset_reset: str = "earliest"
    format: DeserializationFormat = DeserializationFormat.JSON
    offset_policy: OffsetPolicy = OffsetPolicy.AUTO
    max_poll_records: int = 500
    poll_timeout_ms: int = 1000
    session_timeout_ms: int = 30000
    dead_letter_topic: str | None = None
    schema_registry_url: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class ConsumerMetrics:
    """Metrics for tracking consumer performance."""

    messages_consumed: int = 0
    messages_failed: int = 0
    dead_letter_count: int = 0
    last_offset: dict[str, int] = field(default_factory=dict)
    started_at: float = field(default_factory=time.monotonic)

    @property
    def uptime_seconds(self) -> float:
        return time.monotonic() - self.started_at

    def to_dict(self) -> dict[str, Any]:
        return {
            "messages_consumed": self.messages_consumed,
            "messages_failed": self.messages_failed,
            "dead_letter_count": self.dead_letter_count,
            "last_offset": dict(self.last_offset),
            "uptime_seconds": round(self.uptime_seconds, 2),
        }


class KafkaConsumer:
    """Kafka consumer that reads from topics and feeds records into a pipeline.

    Supports JSON and Avro deserialization, configurable offset management,
    dead letter topic routing for failed records, and graceful shutdown.

    Parameters
    ----------
    config:
        Consumer configuration including brokers, topics, and format.
    on_message:
        Async callback invoked for each batch of deserialized records.
    on_error:
        Optional callback for records that fail deserialization.

    Example::

        config = KafkaConsumerConfig(
            brokers=["localhost:9092"],
            topics=["transactions"],
            consumer_group="etl-pipeline",
            format=DeserializationFormat.JSON,
        )

        async def process(records):
            for r in records:
                print(r)

        consumer = KafkaConsumer(config, on_message=process)
        await consumer.start()
    """

    def __init__(
        self,
        config: KafkaConsumerConfig,
        on_message: Callable[[list[dict[str, Any]]], Any] | None = None,
        on_error: Callable[[bytes, Exception], Any] | None = None,
    ) -> None:
        self._config = config
        self._on_message = on_message
        self._on_error = on_error
        self._running = False
        self._consumer: Any = None
        self._producer: Any = None  # For dead letter topic
        self._metrics = ConsumerMetrics()
        self._shutdown_event = asyncio.Event()

    @property
    def config(self) -> KafkaConsumerConfig:
        return self._config

    @property
    def metrics(self) -> ConsumerMetrics:
        return self._metrics

    @property
    def is_running(self) -> bool:
        return self._running

    def _create_consumer(self) -> Any:
        """Create the underlying confluent-kafka Consumer instance."""
        try:
            from confluent_kafka import Consumer
        except ImportError:
            raise ImportError(
                "confluent-kafka is required for Kafka support. "
                "Install with: pip install pipeline-engine[kafka]"
            )

        conf = {
            "bootstrap.servers": ",".join(self._config.brokers),
            "group.id": self._config.consumer_group,
            "auto.offset.reset": self._config.auto_offset_reset,
            "enable.auto.commit": self._config.offset_policy == OffsetPolicy.AUTO,
            "max.poll.interval.ms": 300000,
            "session.timeout.ms": self._config.session_timeout_ms,
        }
        conf.update(self._config.extra)
        return Consumer(conf)

    def _create_dead_letter_producer(self) -> Any:
        """Create a producer for the dead letter topic."""
        if not self._config.dead_letter_topic:
            return None
        try:
            from confluent_kafka import Producer
        except ImportError:
            return None

        return Producer({
            "bootstrap.servers": ",".join(self._config.brokers),
        })

    def _deserialize(self, raw: bytes) -> dict[str, Any]:
        """Deserialize a raw message according to the configured format."""
        if self._config.format == DeserializationFormat.JSON:
            return json.loads(raw)
        elif self._config.format == DeserializationFormat.AVRO:
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
        elif self._config.format == DeserializationFormat.RAW:
            return {"raw": raw.decode("utf-8", errors="replace")}
        else:
            raise ValueError(f"Unknown format: {self._config.format}")

    def _send_to_dead_letter(self, raw: bytes, error: str) -> None:
        """Route a failed message to the dead letter topic."""
        if not self._producer or not self._config.dead_letter_topic:
            return

        envelope = json.dumps({
            "original_message": raw.decode("utf-8", errors="replace"),
            "error": error,
            "timestamp": time.time(),
            "consumer_group": self._config.consumer_group,
        }).encode("utf-8")

        self._producer.produce(
            self._config.dead_letter_topic,
            value=envelope,
        )
        self._producer.flush(timeout=5)
        self._metrics.dead_letter_count += 1

    async def start(self) -> None:
        """Start consuming messages from Kafka topics.

        Runs until :meth:`stop` is called or a shutdown signal is received.
        """
        self._consumer = self._create_consumer()
        self._producer = self._create_dead_letter_producer()
        self._consumer.subscribe(self._config.topics)
        self._running = True
        self._metrics = ConsumerMetrics()
        self._shutdown_event.clear()

        logger.info(
            "KafkaConsumer started: topics=%s, group=%s",
            self._config.topics,
            self._config.consumer_group,
        )

        try:
            while self._running and not self._shutdown_event.is_set():
                await self._poll_batch()
        finally:
            self._cleanup()

    async def _poll_batch(self) -> None:
        """Poll a batch of messages and dispatch them."""
        messages = await asyncio.to_thread(
            self._consumer.consume,
            num_messages=self._config.max_poll_records,
            timeout=self._config.poll_timeout_ms / 1000.0,
        )

        if not messages:
            return

        records: list[dict[str, Any]] = []
        for msg in messages:
            if msg.error():
                logger.warning("Consumer error: %s", msg.error())
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
                    "timestamp": msg.timestamp(),
                }
                records.append(record)

                # Track offset
                topic_partition = f"{msg.topic()}-{msg.partition()}"
                self._metrics.last_offset[topic_partition] = msg.offset()
            except Exception as e:
                self._metrics.messages_failed += 1
                logger.error("Deserialization failed: %s", e)
                self._send_to_dead_letter(raw, str(e))
                if self._on_error:
                    self._on_error(raw, e)

        if records and self._on_message:
            self._metrics.messages_consumed += len(records)
            await asyncio.to_thread(self._on_message, records) if not asyncio.iscoroutinefunction(self._on_message) else await self._on_message(records)

        if (
            self._config.offset_policy == OffsetPolicy.MANUAL
            and records
        ):
            await asyncio.to_thread(self._consumer.commit)

    def _cleanup(self) -> None:
        """Close consumer and producer connections."""
        self._running = False
        if self._consumer:
            try:
                self._consumer.close()
            except Exception:
                pass
            self._consumer = None
        if self._producer:
            try:
                self._producer.flush(timeout=5)
            except Exception:
                pass
            self._producer = None

    async def stop(self) -> None:
        """Signal the consumer to stop gracefully."""
        logger.info("KafkaConsumer stopping...")
        self._running = False
        self._shutdown_event.set()

    async def __aenter__(self) -> KafkaConsumer:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.stop()

    def __repr__(self) -> str:
        return (
            f"KafkaConsumer(topics={self._config.topics}, "
            f"group={self._config.consumer_group}, "
            f"running={self._running})"
        )
