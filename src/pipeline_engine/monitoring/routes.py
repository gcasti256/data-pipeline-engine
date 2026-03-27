"""FastAPI routes for the pipeline monitoring dashboard."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from pipeline_engine.monitoring.metrics import get_collector

router = APIRouter(tags=["monitoring"])


@router.get("/health")
async def health_check() -> dict[str, str]:
    """Basic health-check endpoint.

    Returns:
        A JSON object with ``status`` and ``version`` fields.
    """
    return {"status": "ok", "version": "0.1.0"}


@router.get("/pipelines")
async def list_pipelines() -> list[dict[str, Any]]:
    """List all recorded pipeline runs.

    Returns run metadata sorted newest-first.  Each entry includes
    ``run_id``, ``pipeline_name``, ``status``, ``created_at``,
    ``duration_seconds``, and a ``summary`` block with node counts.
    """
    collector = get_collector()
    return collector.get_runs()


@router.get("/pipelines/{run_id}")
async def get_pipeline(run_id: str) -> dict[str, Any]:
    """Retrieve detailed status for a specific pipeline run.

    Includes per-node metrics (status, duration, input/output record
    counts, retries, errors).

    Args:
        run_id: The unique run identifier.

    Raises:
        HTTPException: 404 if the run is not found.
    """
    collector = get_collector()
    run = collector.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
    return run


@router.get("/metrics")
async def aggregate_metrics() -> dict[str, Any]:
    """Return aggregate metrics across all recorded pipeline runs.

    Includes total runs, success/error rates, average duration, and
    total records processed.
    """
    collector = get_collector()
    return collector.get_aggregate_metrics()
