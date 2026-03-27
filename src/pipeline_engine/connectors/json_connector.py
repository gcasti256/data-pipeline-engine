from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from .base import BaseConnector, ConnectorConfig


class JSONConnector(BaseConnector):
    """Connector for reading and writing JSON and JSON Lines files.

    Supports nested record extraction via dot-notation paths
    (e.g. ``"data.results"``).
    """

    def __init__(
        self,
        path: str,
        jsonl: bool = False,
        record_path: str | None = None,
        config: ConnectorConfig | None = None,
    ) -> None:
        super().__init__(config)
        self.path = Path(path)
        self.jsonl = jsonl
        self.record_path = record_path

    @staticmethod
    def _extract_path(data: Any, path: str) -> Any:
        """Traverse nested data using dot-separated *path*."""
        current = data
        for segment in path.split("."):
            if isinstance(current, dict):
                current = current[segment]
            elif isinstance(current, (list, tuple)):
                current = current[int(segment)]
            else:
                raise TypeError(
                    f"Cannot traverse into {type(current).__name__} "
                    f"with key '{segment}'"
                )
        return current

    async def read(self, **kwargs: Any) -> list[dict[str, Any]]:
        """Read a JSON or JSONL file and return a list of records."""
        if not self.path.exists():
            raise FileNotFoundError(f"JSON file not found: {self.path}")

        def _read_sync() -> str:
            return self.path.read_text(encoding="utf-8")

        content = await asyncio.to_thread(_read_sync)

        if not content.strip():
            return []

        if self.jsonl:
            records: list[dict[str, Any]] = []
            for line in content.splitlines():
                line = line.strip()
                if line:
                    records.append(json.loads(line))
            return records

        data = json.loads(content)

        if self.record_path:
            data = self._extract_path(data, self.record_path)

        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]

        raise TypeError(
            f"Expected list or dict after parsing, got {type(data).__name__}"
        )

    async def read_stream(
        self, **kwargs: Any
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """Stream records in batches."""
        if not self.jsonl:
            async for batch in super().read_stream(**kwargs):
                yield batch
            return

        if not self.path.exists():
            raise FileNotFoundError(f"JSON file not found: {self.path}")

        def _read_lines() -> list[str]:
            return self.path.read_text(encoding="utf-8").splitlines()

        lines = await asyncio.to_thread(_read_lines)
        batch: list[dict[str, Any]] = []
        for line in lines:
            line = line.strip()
            if not line:
                continue
            batch.append(json.loads(line))
            if len(batch) >= self.config.batch_size:
                yield batch
                batch = []

        if batch:
            yield batch

    async def write(
        self,
        records: list[dict[str, Any]],
        mode: str = "w",
        **kwargs: Any,
    ) -> int:
        """Write records to a JSON or JSONL file."""
        if not records:
            return 0

        def _write_sync() -> None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            if self.jsonl:
                with open(self.path, mode=mode, encoding="utf-8") as f:
                    for record in records:
                        f.write(json.dumps(record, default=str) + "\n")
            else:
                with open(self.path, mode="w", encoding="utf-8") as f:
                    f.write(json.dumps(records, indent=2, default=str) + "\n")

        await asyncio.to_thread(_write_sync)
        return len(records)
