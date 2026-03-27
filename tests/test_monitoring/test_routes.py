"""Tests for pipeline_engine.monitoring.routes — FastAPI monitoring endpoints."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from pipeline_engine.core.state import PipelineState
from pipeline_engine.monitoring.app import create_app
from pipeline_engine.monitoring.metrics import MetricsCollector

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_metrics():
    """Ensure a fresh metrics collector for each test."""
    MetricsCollector.reset()
    yield
    MetricsCollector.reset()


@pytest.fixture
def client():
    """FastAPI TestClient for the monitoring app."""
    app = create_app()
    return TestClient(app)


def _make_completed_state(name: str = "test_pipeline") -> PipelineState:
    """Create a minimal PipelineState that looks completed."""
    from pipeline_engine.core.dag import DAG, Node

    def _noop(ctx):
        return [1, 2, 3]

    dag = DAG(name)
    dag.add_node(Node(id="step1", operation=_noop))
    state = PipelineState(pipeline_name=name)
    state.initialize(dag)
    state.mark_running("step1")
    state.mark_completed("step1", output_records=3)
    return state


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestMonitoringRoutes:
    def test_health_endpoint(self, client):
        """GET /health returns status ok."""
        response = client.get("/health")
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "ok"
        assert "version" in body

    def test_list_pipelines_empty(self, client):
        """GET /pipelines returns empty list when no runs recorded."""
        response = client.get("/pipelines")
        assert response.status_code == 200
        assert response.json() == []

    def test_list_pipelines_with_data(self, client):
        """GET /pipelines returns recorded runs."""
        collector = MetricsCollector.get_instance()
        state = _make_completed_state("my_pipeline")
        collector.record_run(state)

        response = client.get("/pipelines")
        assert response.status_code == 200
        runs = response.json()
        assert len(runs) == 1
        assert runs[0]["pipeline_name"] == "my_pipeline"
        assert runs[0]["status"] == "completed"

    def test_get_pipeline_detail(self, client):
        """GET /pipelines/{run_id} returns details for a specific run."""
        collector = MetricsCollector.get_instance()
        state = _make_completed_state()
        collector.record_run(state)

        response = client.get(f"/pipelines/{state.run_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["run_id"] == state.run_id
        assert "nodes" in data

    def test_get_pipeline_not_found(self, client):
        """GET /pipelines/{run_id} returns 404 for unknown runs."""
        response = client.get("/pipelines/nonexistent")
        assert response.status_code == 404

    def test_get_metrics(self, client):
        """GET /metrics returns aggregate statistics."""
        collector = MetricsCollector.get_instance()
        state = _make_completed_state()
        collector.record_run(state)

        response = client.get("/metrics")
        assert response.status_code == 200
        metrics = response.json()
        assert metrics["total_runs"] == 1
        assert metrics["successful_runs"] == 1
        assert metrics["failed_runs"] == 0
        assert metrics["success_rate"] == 1.0
        assert metrics["total_records_processed"] == 3
