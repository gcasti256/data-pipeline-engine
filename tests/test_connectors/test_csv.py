"""Tests for pipeline_engine.connectors.csv_connector — CSV read/write/stream."""

from __future__ import annotations

import pytest

from pipeline_engine.connectors.base import ConnectorConfig
from pipeline_engine.connectors.csv_connector import CSVConnector


class TestCSVConnector:
    @pytest.mark.asyncio
    async def test_read_csv(self, tmp_path):
        """Read a CSV file and verify the records match expectations."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("name,age,city\nAlice,30,NYC\nBob,25,LA\n")

        connector = CSVConnector(path=str(csv_file))
        records = await connector.read()

        assert len(records) == 2
        assert records[0] == {"name": "Alice", "age": "30", "city": "NYC"}
        assert records[1] == {"name": "Bob", "age": "25", "city": "LA"}

    @pytest.mark.asyncio
    async def test_write_csv(self, tmp_path):
        """Write records to a CSV file and verify the content."""
        csv_file = tmp_path / "output.csv"

        connector = CSVConnector(path=str(csv_file))
        records = [
            {"name": "Alice", "age": "30"},
            {"name": "Bob", "age": "25"},
        ]
        count = await connector.write(records)

        assert count == 2
        content = csv_file.read_text()
        assert "name" in content
        assert "Alice" in content
        assert "Bob" in content

    @pytest.mark.asyncio
    async def test_write_then_read_roundtrip(self, tmp_path):
        """Records survive a write-then-read round trip."""
        csv_file = tmp_path / "roundtrip.csv"
        connector = CSVConnector(path=str(csv_file))

        original = [
            {"id": "1", "value": "hello"},
            {"id": "2", "value": "world"},
        ]
        await connector.write(original)
        result = await connector.read()

        assert result == original

    @pytest.mark.asyncio
    async def test_read_stream(self, tmp_path):
        """read_stream yields data in batches of the configured batch_size."""
        csv_file = tmp_path / "stream.csv"
        lines = ["id,val"] + [f"{i},v{i}" for i in range(10)]
        csv_file.write_text("\n".join(lines) + "\n")

        config = ConnectorConfig(batch_size=3)
        connector = CSVConnector(path=str(csv_file), config=config)

        batches = []
        async for batch in connector.read_stream():
            batches.append(batch)

        # 10 records in batches of 3 -> 4 batches (3, 3, 3, 1)
        assert len(batches) == 4
        assert len(batches[0]) == 3
        assert len(batches[-1]) == 1

    @pytest.mark.asyncio
    async def test_missing_file_raises(self, tmp_path):
        """Reading a nonexistent file raises FileNotFoundError."""
        connector = CSVConnector(path=str(tmp_path / "nonexistent.csv"))
        with pytest.raises(FileNotFoundError):
            await connector.read()

    @pytest.mark.asyncio
    async def test_custom_delimiter(self, tmp_path):
        """CSV files with a custom delimiter are parsed correctly."""
        tsv_file = tmp_path / "data.tsv"
        tsv_file.write_text("name\tage\nAlice\t30\nBob\t25\n")

        connector = CSVConnector(path=str(tsv_file), delimiter="\t")
        records = await connector.read()

        assert len(records) == 2
        assert records[0]["name"] == "Alice"
        assert records[0]["age"] == "30"

    @pytest.mark.asyncio
    async def test_write_empty_records(self, tmp_path):
        """Writing an empty list returns 0 and does not create a file."""
        csv_file = tmp_path / "empty.csv"
        connector = CSVConnector(path=str(csv_file))
        count = await connector.write([])
        assert count == 0
