"""Tests for pipeline_engine.connectors.sqlite_connector — SQLite read/write."""

from __future__ import annotations

import pytest

from pipeline_engine.connectors.sqlite_connector import SQLiteConnector

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSQLiteConnector:
    @pytest.mark.asyncio
    async def test_write_and_read(self, tmp_path):
        """Write records then read them back and verify roundtrip."""
        db_path = str(tmp_path / "test.db")
        connector = SQLiteConnector(database=db_path)

        records = [
            {"id": 1, "name": "Alice", "score": 95.5},
            {"id": 2, "name": "Bob", "score": 87.0},
            {"id": 3, "name": "Charlie", "score": 72.3},
        ]

        try:
            count = await connector.write(records, table="users")
            assert count == 3

            result = await connector.read(table="users")
            assert len(result) == 3
            assert result[0]["name"] == "Alice"
            assert result[2]["score"] == 72.3
        finally:
            await connector.close()

    @pytest.mark.asyncio
    async def test_create_table(self, tmp_path):
        """create_table creates the table and allows subsequent writes."""
        db_path = str(tmp_path / "test.db")
        connector = SQLiteConnector(database=db_path)

        try:
            await connector.create_table(
                "products", {"id": "INTEGER", "name": "TEXT", "price": "REAL"}
            )

            records = [{"id": 1, "name": "Widget", "price": 9.99}]
            count = await connector.write(records, table="products")
            assert count == 1

            result = await connector.read(table="products")
            assert len(result) == 1
            assert result[0]["name"] == "Widget"
        finally:
            await connector.close()

    @pytest.mark.asyncio
    async def test_read_with_query(self, tmp_path):
        """Custom SQL queries return filtered results."""
        db_path = str(tmp_path / "test.db")
        connector = SQLiteConnector(database=db_path)

        try:
            records = [
                {"id": 1, "category": "A", "value": 10},
                {"id": 2, "category": "B", "value": 20},
                {"id": 3, "category": "A", "value": 30},
            ]
            await connector.write(records, table="items")

            result = await connector.read(
                query='SELECT * FROM items WHERE category = ?',
                params=("A",),
            )
            assert len(result) == 2
            assert all(r["category"] == "A" for r in result)
        finally:
            await connector.close()

    @pytest.mark.asyncio
    async def test_if_exists_replace(self, tmp_path):
        """if_exists='replace' drops the existing table before writing."""
        db_path = str(tmp_path / "test.db")
        connector = SQLiteConnector(database=db_path)

        try:
            # First write
            await connector.write(
                [{"id": 1, "val": "old"}], table="data"
            )

            # Replace with new data
            await connector.write(
                [{"id": 10, "val": "new"}, {"id": 20, "val": "newer"}],
                table="data",
                if_exists="replace",
            )

            result = await connector.read(table="data")
            assert len(result) == 2
            assert result[0]["id"] == 10
        finally:
            await connector.close()

    @pytest.mark.asyncio
    async def test_if_exists_fail(self, tmp_path):
        """if_exists='fail' raises ValueError if the table already exists."""
        db_path = str(tmp_path / "test.db")
        connector = SQLiteConnector(database=db_path)

        try:
            await connector.write([{"id": 1}], table="existing")

            with pytest.raises(ValueError, match="already exists"):
                await connector.write(
                    [{"id": 2}], table="existing", if_exists="fail"
                )
        finally:
            await connector.close()

    @pytest.mark.asyncio
    async def test_read_requires_query_or_table(self, tmp_path):
        """read() raises ValueError when neither query nor table is given."""
        db_path = str(tmp_path / "test.db")
        connector = SQLiteConnector(database=db_path)

        try:
            with pytest.raises(ValueError, match="query.*table"):
                await connector.read()
        finally:
            await connector.close()

    @pytest.mark.asyncio
    async def test_write_empty_returns_zero(self, tmp_path):
        """Writing an empty list returns 0."""
        db_path = str(tmp_path / "test.db")
        connector = SQLiteConnector(database=db_path)

        try:
            count = await connector.write([], table="data")
            assert count == 0
        finally:
            await connector.close()
