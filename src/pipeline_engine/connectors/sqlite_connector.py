from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import aiosqlite

from .base import BaseConnector, ConnectorConfig

# Mapping from Python-friendly type names to SQLite column affinities.
_PYTHON_TO_SQLITE: dict[str, str] = {
    "str": "TEXT",
    "int": "INTEGER",
    "float": "REAL",
    "bool": "INTEGER",
    "bytes": "BLOB",
}


class SQLiteConnector(BaseConnector):
    """Connector for reading from and writing to SQLite databases.

    Uses ``aiosqlite`` for non-blocking database access.
    """

    def __init__(
        self,
        database: str,
        config: ConnectorConfig | None = None,
    ) -> None:
        super().__init__(config)
        self.database = database
        self._conn: aiosqlite.Connection | None = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _get_connection(self) -> aiosqlite.Connection:
        if self._conn is None:
            self._conn = await aiosqlite.connect(self.database)
            self._conn.row_factory = aiosqlite.Row  # type: ignore[assignment]
        return self._conn

    @staticmethod
    def _infer_schema(record: dict[str, Any]) -> dict[str, str]:
        """Infer a column-name -> SQLite-type mapping from a sample record."""
        schema: dict[str, str] = {}
        for key, value in record.items():
            py_type = type(value).__name__ if value is not None else "str"
            schema[key] = _PYTHON_TO_SQLITE.get(py_type, "TEXT")
        return schema

    # ------------------------------------------------------------------
    # DDL
    # ------------------------------------------------------------------

    async def create_table(
        self, table: str, schema: dict[str, str]
    ) -> None:
        """Create a table if it does not already exist.

        Args:
            table: Table name.
            schema: Mapping of column names to SQL types
                (e.g. ``{"id": "INTEGER", "name": "TEXT"}``).
        """
        conn = await self._get_connection()
        columns = ", ".join(
            f'"{col}" {dtype}' for col, dtype in schema.items()
        )
        await conn.execute(
            f'CREATE TABLE IF NOT EXISTS "{table}" ({columns})'
        )
        await conn.commit()

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    async def read(
        self,
        query: str = "",
        table: str = "",
        params: tuple[Any, ...] = (),
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Execute a query or select all rows from a table.

        Provide either *query* (arbitrary SQL) or *table* (simple
        ``SELECT *``).  If both are given, *query* takes precedence.

        Args:
            query: Raw SQL query.
            table: Table name for a ``SELECT *`` shorthand.
            params: Bind parameters for the query.

        Returns:
            List of row dicts.

        Raises:
            ValueError: If neither *query* nor *table* is provided.
        """
        if not query and not table:
            raise ValueError("Either 'query' or 'table' must be provided")

        conn = await self._get_connection()
        sql = query if query else f'SELECT * FROM "{table}"'
        cursor = await conn.execute(sql, params)
        rows = await cursor.fetchall()

        if not rows:
            return []

        columns = [desc[0] for desc in cursor.description]  # type: ignore[union-attr]
        return [dict(zip(columns, row)) for row in rows]

    async def read_stream(
        self, **kwargs: Any
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """Stream rows from a query in batches."""
        query: str = kwargs.get("query", "")
        table: str = kwargs.get("table", "")
        params: tuple[Any, ...] = kwargs.get("params", ())

        if not query and not table:
            raise ValueError("Either 'query' or 'table' must be provided")

        conn = await self._get_connection()
        sql = query if query else f'SELECT * FROM "{table}"'
        cursor = await conn.execute(sql, params)

        columns = [desc[0] for desc in cursor.description]  # type: ignore[union-attr]
        batch: list[dict[str, Any]] = []

        while True:
            rows = await cursor.fetchmany(self.config.batch_size)
            if not rows:
                break
            batch = [dict(zip(columns, row)) for row in rows]
            yield batch

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    async def write(
        self,
        records: list[dict[str, Any]],
        table: str = "",
        if_exists: str = "append",
        **kwargs: Any,
    ) -> int:
        """Write records to a SQLite table.

        Args:
            records: Rows to insert.
            table: Target table name (required).
            if_exists: Behaviour when the table exists --
                ``'append'`` (default), ``'replace'``, or ``'fail'``.

        Returns:
            Number of records written.

        Raises:
            ValueError: If *table* is empty or *if_exists* is invalid.
            sqlite3.OperationalError: If *if_exists* is ``'fail'`` and the
                table already exists.
        """
        if not table:
            raise ValueError("'table' is required for write operations")
        if not records:
            return 0
        if if_exists not in ("append", "replace", "fail"):
            raise ValueError(
                f"if_exists must be 'append', 'replace', or 'fail'; "
                f"got '{if_exists}'"
            )

        conn = await self._get_connection()

        # Check table existence.
        cursor = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        )
        table_exists = await cursor.fetchone() is not None

        if table_exists and if_exists == "fail":
            raise ValueError(f"Table '{table}' already exists (if_exists='fail')")

        if table_exists and if_exists == "replace":
            await conn.execute(f'DROP TABLE IF EXISTS "{table}"')
            table_exists = False

        if not table_exists:
            schema = self._infer_schema(records[0])
            await self.create_table(table, schema)

        columns = list(records[0].keys())
        placeholders = ", ".join("?" for _ in columns)
        col_names = ", ".join(f'"{c}"' for c in columns)
        sql = f'INSERT INTO "{table}" ({col_names}) VALUES ({placeholders})'

        rows = [tuple(record.get(c) for c in columns) for record in records]
        await conn.executemany(sql, rows)
        await conn.commit()

        return len(records)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """Close the database connection."""
        if self._conn is not None:
            await self._conn.close()
            self._conn = None
