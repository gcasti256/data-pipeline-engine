"""Streaming consumer that reads, transforms, validates, and writes records."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from pipeline_engine.connectors.base import BaseConnector
from pipeline_engine.streaming.batch_window import BatchWindow
from pipeline_engine.transforms.base import BaseTransform
from pipeline_engine.validation.dead_letter import DeadLetterQueue
from pipeline_engine.validation.schema import SchemaValidator

logger = logging.getLogger(__name__)


@dataclass
class StreamMetrics:
    """Running counters for a streaming consumer session.

    Attributes:
        total_read: Total records read from the source.
        total_written: Total records successfully written to the sink.
        total_failed: Total records routed to the dead letter queue.
        batches_processed: Number of batches processed.
    """

    total_read: int = 0
    total_written: int = 0
    total_failed: int = 0
    batches_processed: int = 0

    def to_dict(self) -> dict[str, int]:
        """Serialize to a plain dict."""
        return {
            "total_read": self.total_read,
            "total_written": self.total_written,
            "total_failed": self.total_failed,
            "batches_processed": self.batches_processed,
        }


class StreamConsumer:
    """End-to-end streaming consumer: read -> transform -> validate -> write.

    The consumer reads batches from a source connector (via
    :meth:`~BaseConnector.read_stream`), applies a chain of transforms,
    optionally validates records against a schema, routes failures to a
    dead letter queue, and writes valid records to a sink connector.

    Parameters
    ----------
    source:
        Connector providing input data.
    transforms:
        Ordered list of transforms applied to each batch.
    sink:
        Connector receiving output records.
    batch_window:
        Optional :class:`BatchWindow` for rebatching records before writing.
        When provided, records are accumulated through the window and flushed
        to the sink in window-sized batches rather than per-source-batch.
    validator:
        Optional :class:`SchemaValidator` applied after transforms.
    dead_letter:
        Optional :class:`DeadLetterQueue` for records that fail validation.
        When no DLQ is provided, invalid records are silently discarded
        (but still counted in metrics).

    Example::

        consumer = StreamConsumer(
            source=CSVConnector("input.csv"),
            transforms=[FilterTransform("amount > 0")],
            sink=JSONConnector("output.json"),
            validator=SchemaValidator(OrderRecord),
            dead_letter=DeadLetterQueue(),
        )
        metrics = await consumer.run()
        print(f"Processed {metrics.total_read}, wrote {metrics.total_written}")
    """

    def __init__(
        self,
        source: BaseConnector,
        transforms: list[BaseTransform],
        sink: BaseConnector,
        batch_window: BatchWindow | None = None,
        validator: SchemaValidator | None = None,
        dead_letter: DeadLetterQueue | None = None,
    ) -> None:
        self._source = source
        self._transforms = list(transforms)
        self._sink = sink
        self._batch_window = batch_window
        self._validator = validator
        self._dead_letter = dead_letter
        self._metrics = StreamMetrics()

    @property
    def metrics(self) -> StreamMetrics:
        """Current streaming metrics."""
        return self._metrics

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    async def run(self) -> StreamMetrics:
        """Execute the streaming pipeline to completion.

        Reads from the source in a streaming fashion, processes each batch,
        and writes results to the sink.  Returns the final metrics.

        Returns:
            :class:`StreamMetrics` with counters for the entire run.
        """
        self._metrics = StreamMetrics()

        logger.info("StreamConsumer starting")

        async for batch in self._source.read_stream():
            self._metrics.total_read += len(batch)
            processed = self._apply_transforms(batch)
            valid_records, failed_count = self._apply_validation(processed)
            self._metrics.total_failed += failed_count

            await self._write_records(valid_records)
            self._metrics.batches_processed += 1

            logger.debug(
                "Batch %d: read=%d, valid=%d, failed=%d",
                self._metrics.batches_processed,
                len(batch),
                len(valid_records),
                failed_count,
            )

        # Flush any remaining records held by the batch window.
        if self._batch_window is not None:
            remaining = self._batch_window.flush()
            if remaining:
                written = await self._sink.write(remaining)
                self._metrics.total_written += written

        logger.info(
            "StreamConsumer finished: %s",
            self._metrics.to_dict(),
        )

        return self._metrics

    # ------------------------------------------------------------------
    # Internal pipeline stages
    # ------------------------------------------------------------------

    def _apply_transforms(
        self, records: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Apply the transform chain to a batch of records."""
        data = records
        for transform in self._transforms:
            data = transform.execute(data)
        return data

    def _apply_validation(
        self, records: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], int]:
        """Validate records and route failures to the dead letter queue.

        Returns:
            A tuple of ``(valid_records, failed_count)``.
        """
        if self._validator is None:
            return records, 0

        result = self._validator.validate(records)

        # Route invalid records to the DLQ.
        for record, error in result.invalid:
            if self._dead_letter is not None:
                self._dead_letter.add(
                    record=record,
                    error=error,
                    source="stream_validation",
                )

        return result.valid, len(result.invalid)

    async def _write_records(self, records: list[dict[str, Any]]) -> None:
        """Write records to the sink, optionally through a batch window."""
        if not records:
            return

        if self._batch_window is not None:
            for record in records:
                batch = self._batch_window.add(record)
                if batch is not None:
                    written = await self._sink.write(batch)
                    self._metrics.total_written += written
        else:
            written = await self._sink.write(records)
            self._metrics.total_written += written

    def __repr__(self) -> str:
        return (
            f"StreamConsumer(source={self._source.__class__.__name__}, "
            f"transforms={len(self._transforms)}, "
            f"sink={self._sink.__class__.__name__})"
        )
