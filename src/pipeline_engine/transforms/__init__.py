"""Data transformation primitives for the pipeline engine.

All transforms inherit from :class:`BaseTransform` and implement the
``execute(data) -> data`` contract.
"""

from __future__ import annotations

from pipeline_engine.transforms.aggregate import AggregateTransform
from pipeline_engine.transforms.base import BaseTransform
from pipeline_engine.transforms.deduplicate import DeduplicateTransform
from pipeline_engine.transforms.filter import FilterTransform
from pipeline_engine.transforms.join import JoinTransform
from pipeline_engine.transforms.map import MapTransform
from pipeline_engine.transforms.window import WindowTransform

__all__: list[str] = [
    "BaseTransform",
    "AggregateTransform",
    "DeduplicateTransform",
    "FilterTransform",
    "JoinTransform",
    "MapTransform",
    "WindowTransform",
]
