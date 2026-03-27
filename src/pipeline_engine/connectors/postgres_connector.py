from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from .base import BaseConnector, ConnectorConfig

try:
    import asyncpg
except ImportError:
    asyncpg = None  # type: ignore[assignment]

# Mapping from Python-friendly type names to PostgreSQL types.
_PYTHON_TO_PG: dict[str, str] = {
    "str": "TEXT",
    "int": "BIGINT",
    "float": "DOUBLE PRECISION",
    "bool": "BOOLEAN",
    "bytes": "BYTEA",
}


def _require_asyncpg() -> None:
    if asyncpg is None:
        raise ImportError(
            "asyncpg is required for PostgresConnector. "
            "Install it with: pip install asyncpg"
        )


class PostgresConnector(BaseConnector):
    """Connector for reading from and writing to PostgreSQL databases.

    Uses ``asyncpg`` for high-performance async access.  If the package is
    not installed, an ``ImportError`` is raised at connection time with
    install instructions.
    """

    def __init__(
        self,
        dsn: str,
        config: ConnectorConfig | None = None,
    ) -> None:
        super().__init__(config)
        self.dsn = dsn
        self._pool: asyncpg.Pool | None = None  # type: ignore[name-defined]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _get_pool(self) -> asyncpg.Pool:  # type: ignore[name-defined]
        _require_asyncpg()
        if self._pool is None or self._pool._closed:  # type: ignore[union-attr]
            self._pool = await asyncpg.create_pool(self.dsn, min_size=1, max_size=10)
        return self._pool  # type: ignore[return-value]

    @staticmethod
    def _infer_schema(record: dict[str, Any]) -> dict[str, str]:
        """Infer a column-name -> PostgreSQL-type mapping from a sample record."""
        schema: dict[str, str] = {}
        for key, value in record.items():
            py_type = type(value).__name__ if value is not None else "str"
            schema[key] = _PYTHON_TO_PG.get(py_type, "TEXT")
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
                (e.g. ``{"id": "BIGINT", "name": "TEXT"}``).
        """
        pool = await self._get_pool()
        columns = ", ".join(
            f'"{col}" {dtype}' for col, dtype in schema.items()
        )
        async with pool.acquire() as conn:
            await conn.execute(
                f'CREATE TABLE IF NOT EXISTS "{table}" ({columns})'
            )

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

        ``asyncpg`` uses ``$1, $2, ...`` style placeholders rather than
        ``%s``.  When using positional params, write your query accordingly.

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

        pool = await self._get_pool()
        sql = query if query else f'SELECT * FROM "{table}"'

        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)

        return [dict(row) for row in rows]

    async def read_stream(
        self, **kwargs: Any
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """Stream rows from a query in batches using a server-side cursor."""
        query: str = kwargs.get("query", "")
        table: str = kwargs.get("table", "")
        params: tuple[Any, ...] = kwargs.get("params", ())

        if not query and not table:
            raise ValueError("Either 'query' or 'table' must be provided")

        pool = await self._get_pool()
        sql = query if query else f'SELECT * FROM "{table}"'

        async with pool.acquire() as conn:
            async with conn.transaction():
                cursor = await conn.cursor(sql, *params)
                while True:
                    rows = await cursor.fetch(self.config.batch_size)
                    if not rows:
                        break
                    yield [dict(row) for row in rows]

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
        """Write records to a PostgreSQL table.

        Args:
            records: Rows to insert.
            table: Target table name (required).
            if_exists: Behaviour when the table exists --
                ``'append'`` (default), ``'replace'``, or ``'fail'``.

        Returns:
            Number of records written.

        Raises:
            ValueError: If *table* is empty or *if_exists* is invalid.
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

        pool = await self._get_pool()

        async with pool.acquire() as conn:
            # Check table existence.
            exists = await conn.fetchval(
                "SELECT EXISTS ("
                "  SELECT 1 FROM information_schema.tables "
                "  WHERE table_name = $1"
                ")",
                table,
            )

            if exists and if_exists == "fail":
                raise ValueError(
                    f"Table '{table}' already exists (if_exists='fail')"
                )

            if exists and if_exists == "replace":
                await conn.execute(f'DROP TABLE IF EXISTS "{table}"')
                exists = False

            if not exists:
                schema = self._infer_schema(records[0])
                columns = ", ".join(
                    f'"{col}" {dtype}' for col, dtype in schema.items()
                )
                await conn.execute(
                    f'CREATE TABLE IF NOT EXISTS "{table}" ({columns})'
                )

            columns_list = list(records[0].keys())
            rows = [
                tuple(record.get(c) for c in columns_list)
                for record in records
            ]

            # Use copy_records_to_table for efficient bulk inserts when
            # the record set is large, falling back to executemany for
            # smaller batches.
            if len(rows) > self.config.batch_size:
                # asyncpg's copy is the fastest path for bulk data.
                await conn.copy_records_to_table(
                    table,
                    records=rows,
                    columns=columns_list,
                )
            else:
                placeholders = ", ".join(
                    f"${i + 1}" for i in range(len(columns_list))
                )
                col_names = ", ".join(f'"{c}"' for c in columns_list)
                sql = (
                    f'INSERT INTO "{table}" ({col_names}) '
                    f"VALUES ({placeholders})"
                )
                await conn.executemany(sql, rows)

        return len(records)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """Close the connection pool."""
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
