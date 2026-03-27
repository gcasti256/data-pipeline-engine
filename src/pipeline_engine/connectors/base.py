from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ConnectorConfig:
    """Base configuration for all connectors."""

    batch_size: int = 1000
    extra: dict[str, Any] = field(default_factory=dict)


class BaseConnector(ABC):
    """Abstract base class for all data connectors."""

    def __init__(self, config: ConnectorConfig | None = None) -> None:
        self.config = config or ConnectorConfig()

    @abstractmethod
    async def read(self, **kwargs: Any) -> list[dict[str, Any]]:
        """Read data and return as list of records."""
        ...

    @abstractmethod
    async def write(self, records: list[dict[str, Any]], **kwargs: Any) -> int:
        """Write records. Returns number of records written."""
        ...

    async def read_stream(self, **kwargs: Any) -> AsyncIterator[list[dict[str, Any]]]:
        """Stream data in batches. Default implementation reads all at once."""
        data = await self.read(**kwargs)
        for i in range(0, len(data), self.config.batch_size):
            yield data[i : i + self.config.batch_size]

    async def close(self) -> None:
        """Cleanup resources."""
        pass

    async def __aenter__(self) -> BaseConnector:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()
