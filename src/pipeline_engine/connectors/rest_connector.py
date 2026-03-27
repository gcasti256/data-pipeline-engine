from __future__ import annotations

import asyncio
from typing import Any

import httpx

from .base import BaseConnector, ConnectorConfig


class RESTConnector(BaseConnector):
    """Connector for reading from and writing to REST APIs.

    Supports bearer-token and basic authentication, automatic pagination,
    and configurable rate limiting.
    """

    def __init__(
        self,
        base_url: str,
        headers: dict[str, str] | None = None,
        auth_type: str | None = None,
        auth_token: str | None = None,
        rate_limit: float = 0,
        config: ConnectorConfig | None = None,
    ) -> None:
        super().__init__(config)
        self.base_url = base_url.rstrip("/")
        self.headers = dict(headers) if headers else {}
        self.auth_type = auth_type
        self.auth_token = auth_token
        self.rate_limit = rate_limit
        self._client: httpx.AsyncClient | None = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_auth_headers(self) -> dict[str, str]:
        """Construct authentication headers from *auth_type* / *auth_token*."""
        if not self.auth_type or not self.auth_token:
            return {}

        auth_type_lower = self.auth_type.lower()
        if auth_type_lower == "bearer":
            return {"Authorization": f"Bearer {self.auth_token}"}
        if auth_type_lower == "basic":
            return {"Authorization": f"Basic {self.auth_token}"}
        if auth_type_lower == "token":
            return {"Authorization": f"Token {self.auth_token}"}
        # Fallback: use the auth_type verbatim as the scheme.
        return {"Authorization": f"{self.auth_type} {self.auth_token}"}

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            merged_headers = {**self.headers, **self._build_auth_headers()}
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                headers=merged_headers,
                timeout=httpx.Timeout(30.0),
            )
        return self._client

    async def _rate_limit_wait(self) -> None:
        if self.rate_limit > 0:
            await asyncio.sleep(self.rate_limit)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    async def read(
        self,
        endpoint: str = "",
        params: dict[str, Any] | None = None,
        paginate: bool = False,
        page_key: str = "page",
        results_key: str = "results",
        max_pages: int = 100,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """GET data from a REST endpoint.

        Args:
            endpoint: URL path relative to *base_url*.
            params: Query parameters.
            paginate: If ``True``, follow pagination using *page_key*.
            page_key: The query-parameter name used for the page number.
            results_key: Key in the response JSON that holds the list of
                records.  Supports dot-notation (e.g. ``"data.items"``).
            max_pages: Safety limit on the number of pages fetched.

        Returns:
            Aggregated list of records across all pages.

        Raises:
            httpx.HTTPStatusError: On non-2xx responses.
        """
        client = self._get_client()
        params = dict(params) if params else {}
        all_records: list[dict[str, Any]] = []

        if not paginate:
            await self._rate_limit_wait()
            response = await client.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()
            return self._extract_results(data, results_key)

        page = params.pop(page_key, 1)
        if isinstance(page, str) and page.isdigit():
            page = int(page)

        for _ in range(max_pages):
            await self._rate_limit_wait()
            request_params = {**params, page_key: page}
            response = await client.get(endpoint, params=request_params)
            response.raise_for_status()
            data = response.json()
            page_records = self._extract_results(data, results_key)

            if not page_records:
                break

            all_records.extend(page_records)
            page += 1  # type: ignore[operator]

        return all_records

    @staticmethod
    def _extract_results(
        data: Any, results_key: str
    ) -> list[dict[str, Any]]:
        """Extract the record list from a response payload.

        *results_key* may use dot-notation (e.g. ``"data.items"``).
        If the top-level response is already a list, it is returned as-is.
        """
        if isinstance(data, list):
            return data

        if not results_key:
            if isinstance(data, dict):
                return [data]
            return []

        current: Any = data
        for segment in results_key.split("."):
            if isinstance(current, dict) and segment in current:
                current = current[segment]
            else:
                return []

        if isinstance(current, list):
            return current
        if isinstance(current, dict):
            return [current]
        return []

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    async def write(
        self,
        records: list[dict[str, Any]],
        endpoint: str = "",
        method: str = "POST",
        **kwargs: Any,
    ) -> int:
        """Send records to a REST endpoint.

        Records are sent in batches determined by ``config.batch_size``.
        Each batch is sent as a JSON array in the request body.

        Args:
            records: Records to send.
            endpoint: URL path relative to *base_url*.
            method: HTTP method -- ``'POST'`` (default) or ``'PUT'``.

        Returns:
            Number of records successfully sent.
        """
        if not records:
            return 0

        client = self._get_client()
        method = method.upper()
        written = 0

        for i in range(0, len(records), self.config.batch_size):
            batch = records[i : i + self.config.batch_size]
            await self._rate_limit_wait()

            if method == "PUT":
                response = await client.put(endpoint, json=batch)
            else:
                response = await client.post(endpoint, json=batch)

            response.raise_for_status()
            written += len(batch)

        return written

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()
            self._client = None
