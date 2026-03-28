"""Tests for the Kafka producer module."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pipeline_engine.streaming.kafka_producer import (
    KafkaProducer,
    KafkaProducerConfig,
    ProducerMetrics,
    SerializationFormat,
)


class TestKafkaProducerConfig:
    def test_default_config(self):
        config = KafkaProducerConfig()
        assert config.brokers == ["localhost:9092"]
        assert config.topic == ""
        assert config.format == SerializationFormat.JSON
        assert config.batch_size == 100
        assert config.acks == "all"
        assert config.retries == 3
        assert config.idempotent is True

    def test_custom_config(self):
        config = KafkaProducerConfig(
            brokers=["broker1:9092"],
            topic="my-topic",
            format=SerializationFormat.AVRO,
            batch_size=500,
            compression_type="gzip",
        )
        assert config.topic == "my-topic"
        assert config.format == SerializationFormat.AVRO
        assert config.compression_type == "gzip"


class TestProducerMetrics:
    def test_initial_metrics(self):
        metrics = ProducerMetrics()
        assert metrics.messages_sent == 0
        assert metrics.messages_failed == 0
        assert metrics.bytes_sent == 0
        assert metrics.delivery_confirmed == 0

    def test_to_dict(self):
        metrics = ProducerMetrics(
            messages_sent=50,
            bytes_sent=2048,
            delivery_confirmed=48,
        )
        d = metrics.to_dict()
        assert d["messages_sent"] == 50
        assert d["bytes_sent"] == 2048
        assert "uptime_seconds" in d


class TestKafkaProducer:
    def test_init(self):
        config = KafkaProducerConfig(topic="test")
        producer = KafkaProducer(config)
        assert producer.config.topic == "test"
        assert producer.metrics.messages_sent == 0

    def test_repr(self):
        config = KafkaProducerConfig(topic="test")
        producer = KafkaProducer(config)
        r = repr(producer)
        assert "test" in r
        assert "json" in r

    def test_serialize_json(self):
        config = KafkaProducerConfig(format=SerializationFormat.JSON)
        producer = KafkaProducer(config)
        result = producer._serialize({"key": "value", "num": 42})
        parsed = json.loads(result)
        assert parsed["key"] == "value"
        assert parsed["num"] == 42

    def test_serialize_raw(self):
        config = KafkaProducerConfig(format=SerializationFormat.RAW)
        producer = KafkaProducer(config)
        result = producer._serialize({"key": "value"})
        assert isinstance(result, bytes)

    def test_serialize_unknown_format(self):
        config = KafkaProducerConfig()
        producer = KafkaProducer(config)
        producer._config.format = "unknown"  # type: ignore
        with pytest.raises(ValueError, match="Unknown format"):
            producer._serialize({"key": "value"})

    def test_delivery_callback_success(self):
        config = KafkaProducerConfig(topic="test")
        producer = KafkaProducer(config)
        mock_msg = MagicMock()
        mock_msg.topic.return_value = "test"

        producer._delivery_callback(None, mock_msg)
        assert producer.metrics.delivery_confirmed == 1
        assert producer.metrics.messages_failed == 0

    def test_delivery_callback_error(self):
        config = KafkaProducerConfig(topic="test")
        producer = KafkaProducer(config)
        mock_msg = MagicMock()
        mock_msg.topic.return_value = "test"

        producer._delivery_callback("some error", mock_msg)
        assert producer.metrics.messages_failed == 1
        assert producer.metrics.delivery_confirmed == 0

    def test_delivery_callback_custom(self):
        calls = []

        def on_delivery(err, msg):
            calls.append((err, msg))

        config = KafkaProducerConfig(topic="test")
        producer = KafkaProducer(config, on_delivery=on_delivery)
        mock_msg = MagicMock()

        producer._delivery_callback(None, mock_msg)
        assert len(calls) == 1

    @patch("pipeline_engine.streaming.kafka_producer.KafkaProducer._create_producer")
    async def test_send(self, mock_create):
        mock_producer = MagicMock()
        mock_producer.produce = MagicMock()
        mock_producer.poll = MagicMock()
        mock_create.return_value = mock_producer

        config = KafkaProducerConfig(topic="test-topic")
        producer = KafkaProducer(config)
        await producer.connect()

        await producer.send({"event": "click", "user_id": "abc"})

        assert producer.metrics.messages_sent == 1
        assert producer.metrics.bytes_sent > 0
        mock_producer.produce.assert_called_once()

    @patch("pipeline_engine.streaming.kafka_producer.KafkaProducer._create_producer")
    async def test_send_with_key(self, mock_create):
        mock_producer = MagicMock()
        mock_producer.produce = MagicMock()
        mock_producer.poll = MagicMock()
        mock_create.return_value = mock_producer

        config = KafkaProducerConfig(topic="test")
        producer = KafkaProducer(config)
        await producer.connect()

        await producer.send({"data": 1}, key="partition-key")
        call_kwargs = mock_producer.produce.call_args[1]
        assert call_kwargs["key"] == b"partition-key"

    @patch("pipeline_engine.streaming.kafka_producer.KafkaProducer._create_producer")
    async def test_send_no_topic_raises(self, mock_create):
        mock_create.return_value = MagicMock()
        config = KafkaProducerConfig(topic="")
        producer = KafkaProducer(config)
        await producer.connect()

        with pytest.raises(ValueError, match="No topic"):
            await producer.send({"data": 1})

    @patch("pipeline_engine.streaming.kafka_producer.KafkaProducer._create_producer")
    async def test_send_batch(self, mock_create):
        mock_producer = MagicMock()
        mock_producer.produce = MagicMock()
        mock_producer.poll = MagicMock()
        mock_create.return_value = mock_producer

        config = KafkaProducerConfig(topic="test")
        producer = KafkaProducer(config)
        await producer.connect()

        records = [{"id": i} for i in range(5)]
        sent = await producer.send_batch(records)

        assert sent == 5
        assert producer.metrics.messages_sent == 5

    @patch("pipeline_engine.streaming.kafka_producer.KafkaProducer._create_producer")
    async def test_flush(self, mock_create):
        mock_producer = MagicMock()
        mock_producer.flush.return_value = 0
        mock_create.return_value = mock_producer

        config = KafkaProducerConfig(topic="test")
        producer = KafkaProducer(config)
        await producer.connect()
        await producer.flush()

        mock_producer.flush.assert_called_once()

    @patch("pipeline_engine.streaming.kafka_producer.KafkaProducer._create_producer")
    async def test_close(self, mock_create):
        mock_producer = MagicMock()
        mock_producer.flush.return_value = 0
        mock_create.return_value = mock_producer

        config = KafkaProducerConfig(topic="test")
        producer = KafkaProducer(config)
        await producer.connect()
        await producer.close()

        assert producer._producer is None

    @patch("pipeline_engine.streaming.kafka_producer.KafkaProducer._create_producer")
    async def test_context_manager(self, mock_create):
        mock_producer = MagicMock()
        mock_producer.flush.return_value = 0
        mock_create.return_value = mock_producer

        config = KafkaProducerConfig(topic="test")
        async with KafkaProducer(config) as producer:
            assert producer._producer is not None
        assert producer._producer is None

    @patch("pipeline_engine.streaming.kafka_producer.KafkaProducer._create_producer")
    async def test_send_batch_with_key_fn(self, mock_create):
        mock_producer = MagicMock()
        mock_producer.produce = MagicMock()
        mock_producer.poll = MagicMock()
        mock_create.return_value = mock_producer

        config = KafkaProducerConfig(topic="test")
        producer = KafkaProducer(config)
        await producer.connect()

        records = [{"user_id": f"u{i}"} for i in range(3)]
        sent = await producer.send_batch(
            records, key_fn=lambda r: r["user_id"]
        )
        assert sent == 3
