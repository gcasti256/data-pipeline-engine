"""Tests for the Kafka source and sink connectors."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pipeline_engine.connectors.base import ConnectorConfig
from pipeline_engine.connectors.kafka_connector import (
    KafkaConnectorConfig,
    KafkaSink,
    KafkaSource,
)


class TestKafkaConnectorConfig:
    def test_default(self):
        config = KafkaConnectorConfig()
        assert config.brokers == ["localhost:9092"]
        assert config.topic == ""
        assert config.format == "json"
        assert config.idempotent is True

    def test_custom(self):
        config = KafkaConnectorConfig(
            brokers=["b1:9092"],
            topic="my-topic",
            consumer_group="cg",
            format="avro",
        )
        assert config.topic == "my-topic"
        assert config.consumer_group == "cg"


class TestKafkaSource:
    def test_init(self):
        source = KafkaSource(
            brokers=["localhost:9092"],
            topic="test-topic",
            consumer_group="test-group",
        )
        assert repr(source) == "KafkaSource(topic='test-topic', group='test-group', format='json')"

    def test_deserialize_json(self):
        source = KafkaSource(topic="test", format="json")
        data = json.dumps({"key": "val"}).encode()
        result = source._deserialize(data)
        assert result["key"] == "val"

    def test_deserialize_raw(self):
        source = KafkaSource(topic="test", format="raw")
        result = source._deserialize(b"hello")
        assert result["raw"] == "hello"

    def test_deserialize_invalid_format(self):
        source = KafkaSource(topic="test", format="unknown")
        with pytest.raises(ValueError, match="Unknown format"):
            source._deserialize(b"data")

    async def test_write_raises(self):
        source = KafkaSource(topic="test")
        with pytest.raises(NotImplementedError, match="read-only"):
            await source.write([{"data": 1}])

    @patch("pipeline_engine.connectors.kafka_connector.KafkaSource._create_consumer")
    async def test_read_returns_records(self, mock_create):
        msg1 = MagicMock()
        msg1.error.return_value = None
        msg1.value.return_value = json.dumps({"id": 1}).encode()
        msg1.topic.return_value = "test"
        msg1.partition.return_value = 0
        msg1.offset.return_value = 0

        msg2 = MagicMock()
        msg2.error.return_value = None
        msg2.value.return_value = json.dumps({"id": 2}).encode()
        msg2.topic.return_value = "test"
        msg2.partition.return_value = 0
        msg2.offset.return_value = 1

        mock_consumer = MagicMock()
        # First poll returns msg1, second returns msg2, then None 3 times
        mock_consumer.poll.side_effect = [msg1, msg2, None, None, None]
        mock_consumer.close = MagicMock()
        mock_create.return_value = mock_consumer

        source = KafkaSource(topic="test", poll_timeout_seconds=0.1)
        records = await source.read()

        assert len(records) == 2
        assert records[0]["id"] == 1
        assert records[1]["id"] == 2
        assert "_kafka_meta" in records[0]

    @patch("pipeline_engine.connectors.kafka_connector.KafkaSource._create_consumer")
    async def test_read_with_max_messages(self, mock_create):
        messages = []
        for i in range(10):
            msg = MagicMock()
            msg.error.return_value = None
            msg.value.return_value = json.dumps({"id": i}).encode()
            msg.topic.return_value = "test"
            msg.partition.return_value = 0
            msg.offset.return_value = i
            messages.append(msg)

        mock_consumer = MagicMock()
        mock_consumer.poll.side_effect = messages + [None, None, None]
        mock_consumer.close = MagicMock()
        mock_create.return_value = mock_consumer

        source = KafkaSource(topic="test", max_messages=3, poll_timeout_seconds=0.1)
        records = await source.read()
        assert len(records) == 3

    @patch("pipeline_engine.connectors.kafka_connector.KafkaSource._create_consumer")
    async def test_read_handles_errors(self, mock_create):
        err_msg = MagicMock()
        err_msg.error.return_value = "Broker not available"

        ok_msg = MagicMock()
        ok_msg.error.return_value = None
        ok_msg.value.return_value = json.dumps({"id": 1}).encode()
        ok_msg.topic.return_value = "test"
        ok_msg.partition.return_value = 0
        ok_msg.offset.return_value = 0

        mock_consumer = MagicMock()
        mock_consumer.poll.side_effect = [err_msg, ok_msg, None, None, None]
        mock_consumer.close = MagicMock()
        mock_create.return_value = mock_consumer

        source = KafkaSource(topic="test", poll_timeout_seconds=0.1)
        records = await source.read()
        assert len(records) == 1

    @patch("pipeline_engine.connectors.kafka_connector.KafkaSource._create_consumer")
    async def test_read_stream(self, mock_create):
        batch = []
        for i in range(3):
            msg = MagicMock()
            msg.error.return_value = None
            msg.value.return_value = json.dumps({"id": i}).encode()
            msg.topic.return_value = "test"
            msg.partition.return_value = 0
            msg.offset.return_value = i
            batch.append(msg)

        mock_consumer = MagicMock()
        mock_consumer.consume.side_effect = [batch, []]
        mock_consumer.close = MagicMock()
        mock_create.return_value = mock_consumer

        source = KafkaSource(topic="test", poll_timeout_seconds=0.1)
        all_records = []
        async for records in source.read_stream():
            all_records.extend(records)

        assert len(all_records) == 3


class TestKafkaSink:
    def test_init(self):
        sink = KafkaSink(
            brokers=["localhost:9092"],
            topic="output-topic",
            format="json",
        )
        assert repr(sink) == "KafkaSink(topic='output-topic', format='json')"

    def test_serialize_json(self):
        sink = KafkaSink(topic="test", format="json")
        data = sink._serialize({"key": "val", "num": 42})
        parsed = json.loads(data)
        assert parsed["key"] == "val"
        assert parsed["num"] == 42

    def test_serialize_strips_internal_fields(self):
        sink = KafkaSink(topic="test", format="json")
        data = sink._serialize({"key": "val", "_kafka_meta": {"topic": "t"}})
        parsed = json.loads(data)
        assert "_kafka_meta" not in parsed
        assert parsed["key"] == "val"

    async def test_read_raises(self):
        sink = KafkaSink(topic="test")
        with pytest.raises(NotImplementedError, match="write-only"):
            await sink.read()

    @patch("pipeline_engine.connectors.kafka_connector.KafkaSink._create_producer")
    async def test_write(self, mock_create):
        mock_producer = MagicMock()
        mock_producer.produce = MagicMock()
        mock_producer.poll = MagicMock()
        mock_create.return_value = mock_producer

        sink = KafkaSink(topic="out-topic", format="json")
        records = [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
        count = await sink.write(records)

        assert count == 2
        assert mock_producer.produce.call_count == 2

    @patch("pipeline_engine.connectors.kafka_connector.KafkaSink._create_producer")
    async def test_write_with_key_field(self, mock_create):
        mock_producer = MagicMock()
        mock_producer.produce = MagicMock()
        mock_producer.poll = MagicMock()
        mock_create.return_value = mock_producer

        sink = KafkaSink(topic="out", key_field="user_id")
        records = [{"user_id": "abc", "event": "click"}]
        await sink.write(records)

        call_kwargs = mock_producer.produce.call_args[1]
        assert call_kwargs["key"] == b"abc"

    @patch("pipeline_engine.connectors.kafka_connector.KafkaSink._create_producer")
    async def test_write_with_condition(self, mock_create):
        mock_producer = MagicMock()
        mock_producer.produce = MagicMock()
        mock_producer.poll = MagicMock()
        mock_create.return_value = mock_producer

        sink = KafkaSink(topic="alerts", condition="amount > 100")
        records = [
            {"amount": 50},   # filtered out
            {"amount": 150},  # passes condition
            {"amount": 200},  # passes condition
        ]
        count = await sink.write(records)
        assert count == 2

    @patch("pipeline_engine.connectors.kafka_connector.KafkaSink._create_producer")
    async def test_close(self, mock_create):
        mock_producer = MagicMock()
        mock_producer.flush.return_value = None
        mock_create.return_value = mock_producer

        sink = KafkaSink(topic="test")
        sink._producer = mock_producer
        await sink.close()

        mock_producer.flush.assert_called_once()
        assert sink._producer is None

    def test_delivery_callback_success(self):
        sink = KafkaSink(topic="test")
        mock_msg = MagicMock()
        sink._delivery_callback(None, mock_msg)
        assert sink._delivery_count == 1

    def test_delivery_callback_error(self):
        sink = KafkaSink(topic="test")
        mock_msg = MagicMock()
        sink._delivery_callback("error", mock_msg)
        assert sink._error_count == 1

    def test_evaluate_condition_true(self):
        sink = KafkaSink(topic="test", condition="value > 10")
        assert sink._evaluate_condition({"value": 20})

    def test_evaluate_condition_false(self):
        sink = KafkaSink(topic="test", condition="value > 10")
        assert not sink._evaluate_condition({"value": 5})

    def test_evaluate_condition_none(self):
        sink = KafkaSink(topic="test")
        assert sink._evaluate_condition({"any": "record"})
