"""Streaming primitives: batch windowing, stream consumption, and Kafka integration."""

from __future__ import annotations

from pipeline_engine.streaming.batch_window import BatchWindow
from pipeline_engine.streaming.consumer import StreamConsumer, StreamMetrics
from pipeline_engine.streaming.kafka_consumer import (
    DeserializationFormat,
    KafkaConsumer,
    KafkaConsumerConfig,
    OffsetPolicy,
)
from pipeline_engine.streaming.kafka_producer import (
    KafkaProducer,
    KafkaProducerConfig,
    SerializationFormat,
)
from pipeline_engine.streaming.schema_registry import (
    CompatibilityLevel,
    SchemaRegistry,
    SchemaRegistryConfig,
    SchemaType,
    SchemaVersion,
)

__all__ = [
    "BatchWindow",
    "CompatibilityLevel",
    "DeserializationFormat",
    "KafkaConsumer",
    "KafkaConsumerConfig",
    "KafkaProducer",
    "KafkaProducerConfig",
    "OffsetPolicy",
    "SchemaRegistry",
    "SchemaRegistryConfig",
    "SchemaType",
    "SchemaVersion",
    "SerializationFormat",
    "StreamConsumer",
    "StreamMetrics",
]
