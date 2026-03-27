from __future__ import annotations

import asyncio
import csv
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from .base import BaseConnector, ConnectorConfig


class CSVConnector(BaseConnector):
    """Connector for reading and writing CSV files.

    Supports streaming large files in batches without loading the entire
    file into memory.
    """

    def __init__(
        self,
        path: str,
        delimiter: str = ",",
        encoding: str = "utf-8",
        has_header: bool = True,
        config: ConnectorConfig | None = None,
    ) -> None:
        super().__init__(config)
        self.path = Path(path)
        self.delimiter = delimiter
        self.encoding = encoding
        self.has_header = has_header

    async def read(self, **kwargs: Any) -> list[dict[str, Any]]:
        """Read the entire CSV file and return as a list of dicts."""
        if not self.path.exists():
            raise FileNotFoundError(f"CSV file not found: {self.path}")

        def _read_sync() -> list[dict[str, Any]]:
            records: list[dict[str, Any]] = []
            with open(self.path, encoding=self.encoding, newline="") as f:
                if self.has_header:
                    reader = csv.DictReader(f, delimiter=self.delimiter)
                    for row in reader:
                        records.append(dict(row))
                else:
                    reader_raw = csv.reader(f, delimiter=self.delimiter)
                    for row in reader_raw:
                        records.append({str(i): v for i, v in enumerate(row)})
            return records

        return await asyncio.to_thread(_read_sync)

    async def read_stream(
        self, **kwargs: Any
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """Stream CSV data in batches without loading the entire file."""
        if not self.path.exists():
            raise FileNotFoundError(f"CSV file not found: {self.path}")

        def _read_all() -> list[dict[str, Any]]:
            records: list[dict[str, Any]] = []
            with open(self.path, encoding=self.encoding, newline="") as f:
                if self.has_header:
                    reader = csv.DictReader(f, delimiter=self.delimiter)
                    for row in reader:
                        records.append(dict(row))
                else:
                    reader_raw = csv.reader(f, delimiter=self.delimiter)
                    for row in reader_raw:
                        records.append({str(i): v for i, v in enumerate(row)})
            return records

        all_records = await asyncio.to_thread(_read_all)
        batch_size = self.config.batch_size
        for i in range(0, len(all_records), batch_size):
            yield all_records[i : i + batch_size]

    async def write(
        self,
        records: list[dict[str, Any]],
        mode: str = "w",
        **kwargs: Any,
    ) -> int:
        """Write records to a CSV file."""
        if not records:
            return 0

        fieldnames = list(records[0].keys())
        write_header = mode == "w" or not self.path.exists()
        if mode == "a" and self.path.exists():
            write_header = False

        def _write_sync() -> None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.path, mode=mode, encoding=self.encoding, newline="") as f:
                writer = csv.DictWriter(
                    f, fieldnames=fieldnames, delimiter=self.delimiter
                )
                if write_header:
                    writer.writeheader()
                for record in records:
                    writer.writerow(record)

        await asyncio.to_thread(_write_sync)
        return len(records)
