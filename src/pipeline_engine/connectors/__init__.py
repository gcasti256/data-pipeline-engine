"""Data pipeline connectors for various data sources and sinks."""

from __future__ import annotations

from .base import BaseConnector, ConnectorConfig
from .csv_connector import CSVConnector
from .json_connector import JSONConnector
from .postgres_connector import PostgresConnector
from .rest_connector import RESTConnector
from .sqlite_connector import SQLiteConnector

__all__ = [
    # Base
    "BaseConnector",
    "ConnectorConfig",
    # File connectors
    "CSVConnector",
    "JSONConnector",
    # Database connectors
    "SQLiteConnector",
    "PostgresConnector",
    # API connectors
    "RESTConnector",
]
