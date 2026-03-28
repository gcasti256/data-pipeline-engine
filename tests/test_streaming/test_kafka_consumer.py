"""Tests for the Kafka consumer module."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pipeline_engine.streaming.kafka_consumer import (
    ConsumerMetrics,
    DeserializationFormat,
    KafkaConsumer,
    KafkaConsumerConfig,
    OffsetPolicy,
)


class TestKafkaConsumerConfig:
    def test_default_config(self):
        config = KafkaConsumerConfig()
        assert config.brokers == ["localhost:9092"]
        assert config.consumer_group == "pipeline-engine"
        assert config.format == DeserializationFormat.JSON
        assert config.offset_policy == OffsetPolicy.AUTO
        assert config.max_poll_records == 500
        assert config.poll_timeout_ms == 1000

    def test_custom_config(self):
        config = KafkaConsumerConfig(
            brokers=["broker1:9092", "broker2:9092"],
            topics=["topic1", "topic2"],
            consumer_group="my-group",
            format=DeserializationFormat.AVRO,
            offset_policy=OffsetPolicy.MANUAL,
            dead_letter_topic="dlq-topic",
        )
        assert len(config.brokers) == 2
        assert len(config.topics) == 2
        assert config.consumer_group == "my-group"
        assert config.format == DeserializationFormat.AVRO
        assert config.dead_letter_topic == "dlq-topic"


class TestConsumerMetrics:
    def test_initial_metrics(self):
        metrics = ConsumerMetrics()
        assert metrics.messages_consumed == 0
        assert metrics.messages_failed == 0
        assert metrics.dead_letter_count == 0
        assert metrics.uptime_seconds >= 0

    def test_to_dict(self):
        metrics = ConsumerMetrics(
            messages_consumed=100,
            messages_failed=5,
            dead_letter_count=3,
        )
        d = metrics.to_dict()
        assert d["messages_consumed"] == 100
        assert d["messages_failed"] == 5
        assert d["dead_letter_count"] == 3
        assert "uptime_seconds" in d


class TestKafkaConsumer:
    def test_init(self):
        config = KafkaConsumerConfig(
            topics=["test-topic"],
            consumer_group="test-group",
        )
        consumer = KafkaConsumer(config)
        assert consumer.config == config
        assert not consumer.is_running
        assert consumer.metrics.messages_consumed == 0

    def test_repr(self):
        config = KafkaConsumerConfig(
            topics=["test-topic"],
            consumer_group="test-group",
        )
        consumer = KafkaConsumer(config)
        r = repr(consumer)
        assert "test-topic" in r
        assert "test-group" in r

    def test_deserialize_json(self):
        config = KafkaConsumerConfig(format=DeserializationFormat.JSON)
        consumer = KafkaConsumer(config)
        raw = json.dumps({"key": "value", "num": 42}).encode()
        result = consumer._deserialize(raw)
        assert result["key"] == "value"
        assert result["num"] == 42

    def test_deserialize_json_invalid(self):
        config = KafkaConsumerConfig(format=DeserializationFormat.JSON)
        consumer = KafkaConsumer(config)
        with pytest.raises(json.JSONDecodeError):
            consumer._deserialize(b"not json")

    def test_deserialize_raw(self):
        config = KafkaConsumerConfig(format=DeserializationFormat.RAW)
        consumer = KafkaConsumer(config)
        result = consumer._deserialize(b"raw message data")
        assert result["raw"] == "raw message data"

    def test_deserialize_unknown_format(self):
        config = KafkaConsumerConfig()
        consumer = KafkaConsumer(config)
        consumer._config.format = "unknown"  # type: ignore
        with pytest.raises(ValueError, match="Unknown format"):
            consumer._deserialize(b"data")

    @patch("pipeline_engine.streaming.kafka_consumer.KafkaConsumer._create_consumer")
    @patch("pipeline_engine.streaming.kafka_consumer.KafkaConsumer._create_dead_letter_producer")
    async def test_start_and_stop(self, mock_dlq, mock_create):
        mock_consumer = MagicMock()
        mock_consumer.consume.return_value = []
        mock_create.return_value = mock_consumer
        mock_dlq.return_value = None

        config = KafkaConsumerConfig(topics=["test"])
        consumer = KafkaConsumer(config)

        # Start in background, stop after brief run
        async def run_and_stop():
            await asyncio.sleep(0.1)
            await consumer.stop()

        task = asyncio.create_task(run_and_stop())
        await consumer.start()
        await task

        assert not consumer.is_running
        mock_consumer.subscribe.assert_called_once_with(["test"])

    @patch("pipeline_engine.streaming.kafka_consumer.KafkaConsumer._create_consumer")
    @patch("pipeline_engine.streaming.kafka_consumer.KafkaConsumer._create_dead_letter_producer")
    async def test_poll_batch_processes_messages(self, mock_dlq, mock_create):
        mock_msg = MagicMock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = json.dumps({"id": 1}).encode()
        mock_msg.topic.return_value = "test"
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 42
        mock_msg.timestamp.return_value = (1, 1234567890)

        mock_consumer = MagicMock()
        mock_consumer.consume.return_value = [mock_msg]
        mock_create.return_value = mock_consumer
        mock_dlq.return_value = None

        received = []

        async def on_message(records):
            received.extend(records)

        config = KafkaConsumerConfig(topics=["test"])
        consumer = KafkaConsumer(config, on_message=on_message)
        consumer._consumer = mock_consumer
        consumer._running = True

        await consumer._poll_batch()

        assert len(received) == 1
        assert received[0]["id"] == 1
        assert received[0]["_kafka_meta"]["topic"] == "test"
        assert received[0]["_kafka_meta"]["offset"] == 42
        assert consumer.metrics.messages_consumed == 1

    @patch("pipeline_engine.streaming.kafka_consumer.KafkaConsumer._create_consumer")
    @patch("pipeline_engine.streaming.kafka_consumer.KafkaConsumer._create_dead_letter_producer")
    async def test_poll_batch_handles_deserialization_error(self, mock_dlq, mock_create):
        mock_msg = MagicMock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = b"not-json"
        mock_msg.topic.return_value = "test"
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 1

        mock_consumer = MagicMock()
        mock_consumer.consume.return_value = [mock_msg]
        mock_create.return_value = mock_consumer
        mock_dlq.return_value = None

        errors = []

        def on_error(raw, exc):
            errors.append((raw, exc))

        config = KafkaConsumerConfig(topics=["test"])
        consumer = KafkaConsumer(config, on_error=on_error)
        consumer._consumer = mock_consumer
        consumer._running = True

        await consumer._poll_batch()

        assert consumer.metrics.messages_failed == 1
        assert len(errors) == 1

    def test_send_to_dead_letter(self):
        config = KafkaConsumerConfig(dead_letter_topic="dlq-test")
        consumer = KafkaConsumer(config)

        mock_producer = MagicMock()
        consumer._producer = mock_producer

        consumer._send_to_dead_letter(b"bad message", "parse error")

        mock_producer.produce.assert_called_once()
        call_args = mock_producer.produce.call_args
        # produce is called with positional args: topic, value=...
        assert call_args[0][0] == "dlq-test" or call_args.kwargs.get("topic") == "dlq-test"
        assert consumer.metrics.dead_letter_count == 1

    def test_send_to_dead_letter_no_producer(self):
        config = KafkaConsumerConfig()
        consumer = KafkaConsumer(config)
        consumer._send_to_dead_letter(b"msg", "err")
        assert consumer.metrics.dead_letter_count == 0

    async def test_context_manager(self):
        config = KafkaConsumerConfig(topics=["test"])
        consumer = KafkaConsumer(config)
        async with consumer:
            assert not consumer.is_running

    def test_manual_offset_policy(self):
        config = KafkaConsumerConfig(offset_policy=OffsetPolicy.MANUAL)
        assert config.offset_policy == OffsetPolicy.MANUAL
