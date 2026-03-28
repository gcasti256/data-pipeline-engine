"""
Kafka Streaming Pipeline Example
=================================

Complete example of an event-driven streaming pipeline that:
1. Consumes raw transaction events from Kafka
2. Validates against a JSON schema
3. Enriches with risk scoring
4. Aggregates by merchant category in tumbling windows
5. Publishes enriched and aggregated results to output topics
6. Routes failed records to a dead letter topic

Prerequisites:
    docker compose up -d  # Start Kafka, Zookeeper, Schema Registry
    pip install pipeline-engine[kafka]

Usage:
    python examples/kafka_streaming_pipeline.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def main() -> None:
    from pipeline_engine.streaming.kafka_consumer import (
        KafkaConsumer,
        KafkaConsumerConfig,
        DeserializationFormat,
    )
    from pipeline_engine.streaming.kafka_producer import (
        KafkaProducer,
        KafkaProducerConfig,
        SerializationFormat,
    )
    from pipeline_engine.streaming.schema_registry import (
        SchemaValidator,
        SchemaType,
    )
    from pipeline_engine.streaming.stream_processor import (
        WindowedAggregator,
        WindowConfig,
        WindowType,
        IdempotentWriter,
    )

    # --- Load transaction schema ---
    with open("examples/schemas/transaction.json") as f:
        tx_schema = json.load(f)

    validator = SchemaValidator(tx_schema, SchemaType.JSON_SCHEMA)

    # --- Set up windowed aggregation ---
    aggregator = WindowedAggregator(
        config=WindowConfig(
            type=WindowType.TUMBLING,
            size_seconds=60,
        ),
        group_by=["merchant_category"],
        aggregations={
            "total_amount": "sum(amount)",
            "tx_count": "count(*)",
            "avg_amount": "avg(amount)",
        },
        timestamp_field="timestamp",
    )

    # --- Set up idempotent writer for exactly-once ---
    dedup = IdempotentWriter(id_field="transaction_id")

    # --- Configure producers ---
    enriched_producer = KafkaProducer(KafkaProducerConfig(
        brokers=["localhost:9092"],
        topic="enriched-transactions",
        format=SerializationFormat.JSON,
        idempotent=True,
    ))

    alerts_producer = KafkaProducer(KafkaProducerConfig(
        brokers=["localhost:9092"],
        topic="fraud-alerts",
        format=SerializationFormat.JSON,
    ))

    dlq_producer = KafkaProducer(KafkaProducerConfig(
        brokers=["localhost:9092"],
        topic="dlq-transactions",
        format=SerializationFormat.JSON,
    ))

    # --- Processing callback ---
    async def process_batch(records: list[dict]) -> None:
        logger.info("Processing batch of %d records", len(records))

        # Step 1: Deduplicate
        unique = dedup.filter_duplicates(records)
        if len(unique) < len(records):
            logger.info("Filtered %d duplicates", len(records) - len(unique))

        valid_records = []
        for record in unique:
            # Step 2: Validate
            result = validator.validate(record)
            if not result.is_valid:
                logger.warning(
                    "Validation failed for %s: %s",
                    record.get("transaction_id", "unknown"),
                    result.errors,
                )
                await dlq_producer.send({
                    "original": record,
                    "errors": result.errors,
                    "stage": "validation",
                })
                continue

            # Step 3: Enrich with risk score
            is_intl = record.get("is_international", False)
            amount = record.get("amount", 0)
            record["risk_score"] = amount * 0.001 if is_intl else amount * 0.0001
            record["enriched_at"] = time.time()

            valid_records.append(record)

            # Step 4: Publish enriched record
            await enriched_producer.send(
                record,
                key=record.get("customer_id"),
            )

        # Step 5: Windowed aggregation
        agg_results = aggregator.process(valid_records)
        for agg in agg_results:
            logger.info(
                "Window closed: category=%s, total=%.2f, count=%d",
                agg.get("merchant_category"),
                agg.get("total_amount", 0),
                agg.get("tx_count", 0),
            )

            # Step 6: Fraud alert if threshold exceeded
            if agg.get("total_amount", 0) > 10000:
                await alerts_producer.send({
                    "alert_type": "high_volume",
                    "merchant_category": agg.get("merchant_category"),
                    "total_amount": agg["total_amount"],
                    "tx_count": agg["tx_count"],
                    "window_start": agg["_window_start"],
                    "window_end": agg["_window_end"],
                    "timestamp": time.time(),
                })

    # --- Configure consumer ---
    consumer_config = KafkaConsumerConfig(
        brokers=["localhost:9092"],
        topics=["raw-transactions"],
        consumer_group="etl-pipeline",
        format=DeserializationFormat.JSON,
        max_poll_records=100,
    )

    consumer = KafkaConsumer(consumer_config, on_message=process_batch)

    logger.info("Starting streaming pipeline...")
    logger.info("Consuming from: raw-transactions")
    logger.info("Producing to: enriched-transactions, fraud-alerts, dlq-transactions")

    try:
        await consumer.start()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await consumer.stop()
        # Flush any remaining windows
        remaining = aggregator.flush()
        for agg in remaining:
            logger.info("Final window: %s", agg)
        await enriched_producer.close()
        await alerts_producer.close()
        await dlq_producer.close()

    logger.info("Pipeline stopped. Consumer metrics: %s", consumer.metrics.to_dict())


if __name__ == "__main__":
    asyncio.run(main())
