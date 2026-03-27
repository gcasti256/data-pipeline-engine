"""Tests for pipeline_engine.connectors.rest_connector — HTTP read/write with mocking."""

from __future__ import annotations

import httpx
import pytest
import respx

from pipeline_engine.connectors.rest_connector import RESTConnector

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestRESTConnector:
    @pytest.mark.asyncio
    @respx.mock
    async def test_read_get(self):
        """A simple GET returns the expected records."""
        route = respx.get("https://api.example.com/items").mock(
            return_value=httpx.Response(
                200,
                json=[{"id": 1, "name": "A"}, {"id": 2, "name": "B"}],
            )
        )

        connector = RESTConnector(base_url="https://api.example.com")
        try:
            records = await connector.read(endpoint="/items")
        finally:
            await connector.close()

        assert len(records) == 2
        assert records[0]["id"] == 1
        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_read_get_with_results_key(self):
        """When the response wraps records in a key, results_key extracts them."""
        respx.get("https://api.example.com/users").mock(
            return_value=httpx.Response(
                200,
                json={"results": [{"name": "Alice"}], "total": 1},
            )
        )

        connector = RESTConnector(base_url="https://api.example.com")
        try:
            records = await connector.read(
                endpoint="/users", results_key="results"
            )
        finally:
            await connector.close()

        assert len(records) == 1
        assert records[0]["name"] == "Alice"

    @pytest.mark.asyncio
    @respx.mock
    async def test_write_post(self):
        """Writing records sends a POST with JSON body."""
        route = respx.post("https://api.example.com/data").mock(
            return_value=httpx.Response(201, json={"status": "ok"})
        )

        connector = RESTConnector(base_url="https://api.example.com")
        try:
            count = await connector.write(
                [{"key": "val1"}, {"key": "val2"}],
                endpoint="/data",
            )
        finally:
            await connector.close()

        assert count == 2
        assert route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_pagination(self):
        """Paginated reads aggregate records across multiple pages."""
        respx.get("https://api.example.com/items", params={"page": 1}).mock(
            return_value=httpx.Response(
                200, json={"results": [{"id": 1}, {"id": 2}]}
            )
        )
        respx.get("https://api.example.com/items", params={"page": 2}).mock(
            return_value=httpx.Response(
                200, json={"results": [{"id": 3}]}
            )
        )
        respx.get("https://api.example.com/items", params={"page": 3}).mock(
            return_value=httpx.Response(
                200, json={"results": []}
            )
        )

        connector = RESTConnector(base_url="https://api.example.com")
        try:
            records = await connector.read(
                endpoint="/items",
                paginate=True,
                page_key="page",
                results_key="results",
            )
        finally:
            await connector.close()

        assert len(records) == 3
        assert records[0]["id"] == 1
        assert records[-1]["id"] == 3

    @pytest.mark.asyncio
    @respx.mock
    async def test_auth_bearer_header(self):
        """Bearer auth sets the Authorization header correctly."""
        route = respx.get("https://api.example.com/secure").mock(
            return_value=httpx.Response(200, json=[])
        )

        connector = RESTConnector(
            base_url="https://api.example.com",
            auth_type="bearer",
            auth_token="my-secret-token",
        )
        try:
            await connector.read(endpoint="/secure")
        finally:
            await connector.close()

        assert route.called
        request = route.calls[0].request
        assert request.headers["authorization"] == "Bearer my-secret-token"
