"""FastAPI application factory for the pipeline monitoring dashboard."""

from __future__ import annotations

from fastapi import FastAPI

from .routes import router


def create_app() -> FastAPI:
    """Create and configure the monitoring FastAPI application.

    Returns:
        A :class:`FastAPI` instance with monitoring routes mounted.
    """
    app = FastAPI(title="Pipeline Engine Monitor", version="0.1.0")
    app.include_router(router)
    return app


app = create_app()
