"""Streaming primitives: batch windowing, Kafka, schema registry, and stream processing."""

from __future__ import annotations

from pipeline_engine.streaming.batch_window import BatchWindow
from pipeline_engine.streaming.consumer import StreamConsumer, StreamMetrics
from pipeline_engine.streaming.kafka_consumer import (
    ConsumerMetrics,
    DeserializationFormat,
    KafkaConsumer,
    KafkaConsumerConfig,
    OffsetPolicy,
)
from pipeline_engine.streaming.kafka_producer import (
    KafkaProducer,
    KafkaProducerConfig,
    ProducerMetrics,
    SerializationFormat,
)
from pipeline_engine.streaming.schema_registry import (
    CompatibilityLevel,
    SchemaRegistryClient,
    SchemaType,
    SchemaValidator as StreamSchemaValidator,
    SchemaVersion,
    ValidationResult as StreamValidationResult,
)
from pipeline_engine.streaming.stream_processor import (
    IdempotentWriter,
    StreamJoin,
    WatermarkState,
    WindowConfig,
    WindowedAggregator,
    WindowState,
    WindowType,
)

__all__ = [
    # Batch windowing
    "BatchWindow",
    # Stream consumer
    "StreamConsumer",
    "StreamMetrics",
    # Kafka consumer
    "KafkaConsumer",
    "KafkaConsumerConfig",
    "ConsumerMetrics",
    "DeserializationFormat",
    "OffsetPolicy",
    # Kafka producer
    "KafkaProducer",
    "KafkaProducerConfig",
    "ProducerMetrics",
    "SerializationFormat",
    # Schema registry
    "SchemaRegistryClient",
    "SchemaType",
    "SchemaVersion",
    "StreamSchemaValidator",
    "StreamValidationResult",
    "CompatibilityLevel",
    # Stream processing
    "WindowedAggregator",
    "WindowConfig",
    "WindowState",
    "WindowType",
    "WatermarkState",
    "StreamJoin",
    "IdempotentWriter",
]
