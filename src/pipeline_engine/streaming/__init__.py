"""Streaming primitives: batch windowing and end-to-end stream consumption."""

from __future__ import annotations

from pipeline_engine.streaming.batch_window import BatchWindow
from pipeline_engine.streaming.consumer import StreamConsumer, StreamMetrics

__all__ = [
    "BatchWindow",
    "StreamConsumer",
    "StreamMetrics",
]
