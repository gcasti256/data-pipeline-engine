"""Pipeline monitoring: metrics collection and HTTP dashboard."""

from __future__ import annotations

from pipeline_engine.monitoring.app import create_app
from pipeline_engine.monitoring.metrics import MetricsCollector, get_collector

__all__ = [
    "MetricsCollector",
    "create_app",
    "get_collector",
]
