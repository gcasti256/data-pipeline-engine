"""Tests for pipeline_engine.connectors.json_connector — JSON and JSONL read/write."""

from __future__ import annotations

import json

import pytest

from pipeline_engine.connectors.json_connector import JSONConnector

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestJSONConnector:
    @pytest.mark.asyncio
    async def test_read_json(self, tmp_path):
        """Read a standard JSON array file."""
        json_file = tmp_path / "data.json"
        data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        json_file.write_text(json.dumps(data))

        connector = JSONConnector(path=str(json_file))
        records = await connector.read()

        assert len(records) == 2
        assert records[0]["name"] == "Alice"
        assert records[1]["age"] == 25

    @pytest.mark.asyncio
    async def test_read_json_single_object(self, tmp_path):
        """A JSON file with a single object returns a one-element list."""
        json_file = tmp_path / "single.json"
        json_file.write_text(json.dumps({"key": "value"}))

        connector = JSONConnector(path=str(json_file))
        records = await connector.read()

        assert records == [{"key": "value"}]

    @pytest.mark.asyncio
    async def test_read_jsonl(self, tmp_path):
        """Read a JSONL (newline-delimited) file."""
        jsonl_file = tmp_path / "data.jsonl"
        lines = [
            json.dumps({"id": 1, "val": "a"}),
            json.dumps({"id": 2, "val": "b"}),
            json.dumps({"id": 3, "val": "c"}),
        ]
        jsonl_file.write_text("\n".join(lines) + "\n")

        connector = JSONConnector(path=str(jsonl_file), jsonl=True)
        records = await connector.read()

        assert len(records) == 3
        assert records[0]["id"] == 1
        assert records[2]["val"] == "c"

    @pytest.mark.asyncio
    async def test_write_json(self, tmp_path):
        """Write records to a JSON file and verify contents."""
        json_file = tmp_path / "output.json"

        connector = JSONConnector(path=str(json_file))
        records = [{"a": 1}, {"a": 2}]
        count = await connector.write(records)

        assert count == 2
        content = json.loads(json_file.read_text())
        assert len(content) == 2

    @pytest.mark.asyncio
    async def test_write_jsonl(self, tmp_path):
        """Write records to a JSONL file and verify each line is valid JSON."""
        jsonl_file = tmp_path / "output.jsonl"

        connector = JSONConnector(path=str(jsonl_file), jsonl=True)
        records = [{"x": 10}, {"x": 20}]
        count = await connector.write(records)

        assert count == 2
        lines = [ln for ln in jsonl_file.read_text().strip().splitlines() if ln.strip()]
        assert len(lines) == 2
        assert json.loads(lines[0]) == {"x": 10}

    @pytest.mark.asyncio
    async def test_nested_path_extraction(self, tmp_path):
        """record_path extracts nested data from the JSON structure."""
        json_file = tmp_path / "nested.json"
        data = {
            "meta": {"page": 1},
            "data": {
                "results": [
                    {"id": 1, "name": "item1"},
                    {"id": 2, "name": "item2"},
                ]
            },
        }
        json_file.write_text(json.dumps(data))

        connector = JSONConnector(path=str(json_file), record_path="data.results")
        records = await connector.read()

        assert len(records) == 2
        assert records[0]["id"] == 1
        assert records[1]["name"] == "item2"

    @pytest.mark.asyncio
    async def test_missing_file_raises(self, tmp_path):
        """Reading a nonexistent JSON file raises FileNotFoundError."""
        connector = JSONConnector(path=str(tmp_path / "gone.json"))
        with pytest.raises(FileNotFoundError):
            await connector.read()

    @pytest.mark.asyncio
    async def test_read_empty_file(self, tmp_path):
        """Reading an empty file returns an empty list."""
        json_file = tmp_path / "empty.json"
        json_file.write_text("")

        connector = JSONConnector(path=str(json_file))
        records = await connector.read()
        assert records == []
