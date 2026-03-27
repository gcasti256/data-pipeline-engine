"""Pipeline metadata storage using SQLite via aiosqlite."""

from __future__ import annotations

import json
import logging
from typing import Any

import aiosqlite

from pipeline_engine.core.state import PipelineState

logger = logging.getLogger(__name__)

_CREATE_PIPELINE_RUNS = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id       TEXT PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    status       TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    started_at   TEXT,
    completed_at TEXT,
    duration_seconds REAL,
    summary      TEXT
)
"""

_CREATE_NODE_RUNS = """
CREATE TABLE IF NOT EXISTS node_runs (
    run_id       TEXT NOT NULL,
    node_id      TEXT NOT NULL,
    status       TEXT NOT NULL,
    started_at   TEXT,
    completed_at TEXT,
    error        TEXT,
    input_records  INTEGER DEFAULT 0,
    output_records INTEGER DEFAULT 0,
    retries_used   INTEGER DEFAULT 0,
    duration_seconds REAL,
    PRIMARY KEY (run_id, node_id),
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
)
"""


class PipelineDB:
    """Async SQLite store for pipeline execution metadata.

    Provides persistence so that CLI commands like ``pipeline status`` and
    ``pipeline list`` can display historical run data.

    Parameters
    ----------
    db_path:
        Path to the SQLite database file.  Created automatically if it
        does not exist.

    Example::

        db = PipelineDB("pipeline_runs.db")
        await db.init()
        await db.save_run(state)
        recent = await db.list_runs(limit=10)
    """

    def __init__(self, db_path: str = "pipeline_runs.db") -> None:
        self._db_path = db_path
        self._conn: aiosqlite.Connection | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def init(self) -> None:
        """Create the database tables if they do not exist.

        Must be called before any other method.
        """
        self._conn = await aiosqlite.connect(self._db_path)
        await self._conn.execute(_CREATE_PIPELINE_RUNS)
        await self._conn.execute(_CREATE_NODE_RUNS)
        await self._conn.commit()
        logger.info("PipelineDB initialized at %s", self._db_path)

    async def close(self) -> None:
        """Close the database connection."""
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def __aenter__(self) -> PipelineDB:
        await self.init()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    async def save_run(self, state: PipelineState) -> None:
        """Persist a pipeline run and its node states.

        Uses ``INSERT OR REPLACE`` so the same run can be saved multiple
        times (e.g. after each node completes for incremental updates).

        Args:
            state: The :class:`PipelineState` to persist.
        """
        conn = self._get_conn()
        state_dict = state.to_dict()

        await conn.execute(
            """
            INSERT OR REPLACE INTO pipeline_runs
                (run_id, pipeline_name, status, created_at, started_at,
                 completed_at, duration_seconds, summary)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                state.run_id,
                state.pipeline_name,
                state_dict["status"],
                state_dict["created_at"],
                state_dict["started_at"],
                state_dict["completed_at"],
                state_dict["duration_seconds"],
                json.dumps(state_dict["summary"]),
            ),
        )

        for node_id, ns in state.node_states.items():
            ns_dict = ns.to_dict()
            await conn.execute(
                """
                INSERT OR REPLACE INTO node_runs
                    (run_id, node_id, status, started_at, completed_at,
                     error, input_records, output_records, retries_used,
                     duration_seconds)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    state.run_id,
                    node_id,
                    ns_dict["status"],
                    ns_dict["started_at"],
                    ns_dict["completed_at"],
                    ns_dict["error"],
                    ns_dict["input_records"],
                    ns_dict["output_records"],
                    ns_dict["retries_used"],
                    ns_dict["duration_seconds"],
                ),
            )

        await conn.commit()
        logger.debug("Saved run %s to database", state.run_id)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    async def get_run(self, run_id: str) -> dict[str, Any] | None:
        """Retrieve a pipeline run with its node states.

        Args:
            run_id: The pipeline run identifier.

        Returns:
            A dict with run metadata and a ``nodes`` mapping, or ``None``
            if the run does not exist.
        """
        conn = self._get_conn()

        cursor = await conn.execute(
            "SELECT * FROM pipeline_runs WHERE run_id = ?", (run_id,)
        )
        row = await cursor.fetchone()
        if row is None:
            return None

        run_data = self._row_to_run_dict(cursor, row)

        # Fetch node states.
        node_cursor = await conn.execute(
            "SELECT * FROM node_runs WHERE run_id = ? ORDER BY node_id",
            (run_id,),
        )
        node_rows = await node_cursor.fetchall()
        run_data["nodes"] = {
            self._column_value(node_cursor, nr, "node_id"): self._row_to_node_dict(
                node_cursor, nr
            )
            for nr in node_rows
        }

        return run_data

    async def list_runs(self, limit: int = 50) -> list[dict[str, Any]]:
        """List recent pipeline runs.

        Args:
            limit: Maximum number of runs to return.

        Returns:
            A list of run dicts (without node details), newest first.
        """
        conn = self._get_conn()
        cursor = await conn.execute(
            "SELECT * FROM pipeline_runs ORDER BY created_at DESC LIMIT ?",
            (limit,),
        )
        rows = await cursor.fetchall()
        return [self._row_to_run_dict(cursor, row) for row in rows]

    async def get_recent_runs(
        self,
        pipeline_name: str,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """List recent runs for a specific pipeline.

        Args:
            pipeline_name: The pipeline name to filter on.
            limit: Maximum number of runs to return.

        Returns:
            A list of run dicts, newest first.
        """
        conn = self._get_conn()
        cursor = await conn.execute(
            """
            SELECT * FROM pipeline_runs
            WHERE pipeline_name = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (pipeline_name, limit),
        )
        rows = await cursor.fetchall()
        return [self._row_to_run_dict(cursor, row) for row in rows]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_conn(self) -> aiosqlite.Connection:
        """Return the active connection, raising if not initialized."""
        if self._conn is None:
            raise RuntimeError(
                "PipelineDB not initialized. Call 'await db.init()' first."
            )
        return self._conn

    @staticmethod
    def _column_value(cursor: Any, row: Any, column: str) -> Any:
        """Extract a column value from a row by column name."""
        columns = [desc[0] for desc in cursor.description]
        idx = columns.index(column)
        return row[idx]

    @staticmethod
    def _row_to_run_dict(cursor: Any, row: Any) -> dict[str, Any]:
        """Convert a pipeline_runs row to a dict."""
        columns = [desc[0] for desc in cursor.description]
        data = dict(zip(columns, row))
        # Parse the JSON summary back into a dict.
        if data.get("summary") and isinstance(data["summary"], str):
            try:
                data["summary"] = json.loads(data["summary"])
            except json.JSONDecodeError:
                pass
        return data

    @staticmethod
    def _row_to_node_dict(cursor: Any, row: Any) -> dict[str, Any]:
        """Convert a node_runs row to a dict."""
        columns = [desc[0] for desc in cursor.description]
        data = dict(zip(columns, row))
        # Remove run_id from node data (redundant in nested context).
        data.pop("run_id", None)
        return data

    def __repr__(self) -> str:
        return f"PipelineDB(db_path={self._db_path!r})"
