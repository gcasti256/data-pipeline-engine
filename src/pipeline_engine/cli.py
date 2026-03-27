"""Click CLI for the pipeline engine."""

from __future__ import annotations

import asyncio
import sys
from typing import Any

import click
from rich.console import Console
from rich.table import Table

from pipeline_engine.config.parser import build_dag, parse_config, validate_config
from pipeline_engine.core.executor import PipelineExecutor
from pipeline_engine.db import PipelineDB
from pipeline_engine.monitoring.metrics import get_collector

console = Console()

# Default database path for CLI operations.
_DEFAULT_DB = "pipeline_runs.db"


def _run_async(coro: Any) -> Any:
    """Bridge synchronous Click commands to async coroutines."""
    return asyncio.run(coro)


@click.group()
@click.version_option(version="0.1.0", prog_name="pipeline")
def cli() -> None:
    """Pipeline Engine CLI -- run, validate, and monitor data pipelines."""


@cli.command()
@click.option(
    "--config",
    "config_path",
    required=True,
    type=click.Path(exists=True),
    help="Path to the YAML pipeline configuration file.",
)
@click.option(
    "--db",
    "db_path",
    default=_DEFAULT_DB,
    show_default=True,
    help="Path to the SQLite database for persisting run metadata.",
)
@click.option(
    "--max-parallel",
    default=4,
    show_default=True,
    type=int,
    help="Maximum number of parallel node executions.",
)
def run(config_path: str, db_path: str, max_parallel: int) -> None:
    """Parse config, build DAG, execute pipeline, and save results."""
    _run_async(_run_pipeline(config_path, db_path, max_parallel))


async def _run_pipeline(config_path: str, db_path: str, max_parallel: int) -> None:
    """Async implementation of the ``run`` command."""
    # Parse and validate.
    console.print(f"[bold]Loading config:[/bold] {config_path}")
    try:
        config = parse_config(config_path)
    except Exception as exc:
        console.print(f"[red]Failed to parse config:[/red] {exc}")
        sys.exit(1)

    warnings = validate_config(config)
    if warnings:
        console.print("[yellow]Configuration warnings:[/yellow]")
        for w in warnings:
            console.print(f"  [yellow]- {w}[/yellow]")

    # Build DAG.
    console.print(f"[bold]Building DAG:[/bold] {config.name}")
    try:
        dag, context = build_dag(config)
    except Exception as exc:
        console.print(f"[red]Failed to build DAG:[/red] {exc}")
        sys.exit(1)

    console.print(
        f"  Nodes: {len(dag)}, Edges: {len(dag.edges)}, "
        f"Max parallel: {max_parallel}"
    )

    # Execute.
    console.print("[bold]Executing pipeline...[/bold]")
    executor = PipelineExecutor(dag, max_parallel=max_parallel)
    state = await executor.execute()

    # Record metrics.
    collector = get_collector()
    collector.record_run(state)

    # Persist.
    db = PipelineDB(db_path)
    await db.init()
    try:
        await db.save_run(state)
    finally:
        await db.close()

    # Print summary.
    _print_run_summary(state.to_dict())


@cli.command()
@click.option(
    "--config",
    "config_path",
    required=True,
    type=click.Path(exists=True),
    help="Path to the YAML pipeline configuration file.",
)
def validate(config_path: str) -> None:
    """Validate a pipeline configuration file."""
    try:
        config = parse_config(config_path)
    except Exception as exc:
        console.print(f"[red]Failed to parse config:[/red] {exc}")
        sys.exit(1)

    warnings = validate_config(config)
    if warnings:
        console.print("[yellow]Warnings:[/yellow]")
        for w in warnings:
            console.print(f"  [yellow]- {w}[/yellow]")
    else:
        console.print("[green]Configuration is valid. No warnings.[/green]")

    console.print()
    console.print(f"[bold]Pipeline:[/bold] {config.name} (v{config.version})")
    console.print(f"  Sources:    {len(config.sources)}")
    console.print(f"  Transforms: {len(config.transforms)}")
    console.print(f"  Sinks:      {len(config.sinks)}")


@cli.command()
@click.option(
    "--run-id",
    required=True,
    help="Pipeline run ID to look up.",
)
@click.option(
    "--db",
    "db_path",
    default=_DEFAULT_DB,
    show_default=True,
    help="Path to the SQLite database.",
)
def status(run_id: str, db_path: str) -> None:
    """Show detailed status for a pipeline run."""
    _run_async(_show_status(run_id, db_path))


async def _show_status(run_id: str, db_path: str) -> None:
    """Async implementation of the ``status`` command."""
    db = PipelineDB(db_path)
    await db.init()
    try:
        run_data = await db.get_run(run_id)
    finally:
        await db.close()

    if run_data is None:
        console.print(f"[red]Run '{run_id}' not found.[/red]")
        sys.exit(1)

    _print_run_detail(run_data)


@cli.command(name="list")
@click.option(
    "--limit",
    default=20,
    show_default=True,
    type=int,
    help="Maximum number of runs to display.",
)
@click.option(
    "--db",
    "db_path",
    default=_DEFAULT_DB,
    show_default=True,
    help="Path to the SQLite database.",
)
def list_runs(limit: int, db_path: str) -> None:
    """List recent pipeline runs."""
    _run_async(_list_runs(limit, db_path))


async def _list_runs(limit: int, db_path: str) -> None:
    """Async implementation of the ``list`` command."""
    db = PipelineDB(db_path)
    await db.init()
    try:
        runs = await db.list_runs(limit=limit)
    finally:
        await db.close()

    if not runs:
        console.print("[dim]No pipeline runs found.[/dim]")
        return

    table = Table(title="Pipeline Runs")
    table.add_column("Run ID", style="cyan", no_wrap=True, max_width=12)
    table.add_column("Pipeline", style="bold")
    table.add_column("Status")
    table.add_column("Created", style="dim")
    table.add_column("Duration", justify="right")

    for run_data in runs:
        status_str = _status_style(run_data.get("status", ""))
        duration = run_data.get("duration_seconds")
        duration_str = f"{duration:.2f}s" if duration is not None else "-"
        created = run_data.get("created_at", "-")
        if isinstance(created, str) and len(created) > 19:
            created = created[:19]

        table.add_row(
            run_data.get("run_id", "")[:12],
            run_data.get("pipeline_name", ""),
            status_str,
            created,
            duration_str,
        )

    console.print(table)


@cli.command()
@click.option("--host", default="0.0.0.0", show_default=True, help="Bind host.")
@click.option("--port", default=8080, show_default=True, type=int, help="Bind port.")
def monitor(host: str, port: int) -> None:
    """Start the monitoring dashboard HTTP server."""
    try:
        import uvicorn
    except ImportError:
        console.print(
            "[red]uvicorn is required for the monitoring dashboard. "
            "Install it with: pip install uvicorn[/red]"
        )
        sys.exit(1)

    console.print(
        f"[bold]Starting monitoring dashboard at "
        f"http://{host}:{port}[/bold]"
    )
    uvicorn.run(
        "pipeline_engine.monitoring.app:app",
        host=host,
        port=port,
        log_level="info",
    )


def _status_style(status_value: str) -> str:
    """Wrap a status string in Rich markup for coloring."""
    styles: dict[str, str] = {
        "completed": "[green]completed[/green]",
        "failed": "[red]failed[/red]",
        "running": "[blue]running[/blue]",
        "pending": "[dim]pending[/dim]",
        "cancelled": "[yellow]cancelled[/yellow]",
    }
    return styles.get(status_value, status_value)


def _print_run_summary(run_data: dict[str, Any]) -> None:
    """Print a compact summary after a pipeline run."""
    console.print()
    status_str = _status_style(run_data.get("status", ""))
    console.print(f"[bold]Result:[/bold] {status_str}")
    console.print(f"  Run ID:   {run_data.get('run_id', '-')}")
    console.print(f"  Pipeline: {run_data.get('pipeline_name', '-')}")

    duration = run_data.get("duration_seconds")
    if duration is not None:
        console.print(f"  Duration: {duration:.2f}s")

    summary = run_data.get("summary", {})
    if summary:
        console.print(
            f"  Nodes: {summary.get('total', 0)} total, "
            f"{summary.get('completed', 0)} completed, "
            f"{summary.get('failed', 0)} failed"
        )

    # Node details table.
    nodes = run_data.get("nodes", {})
    if nodes:
        console.print()
        table = Table(title="Node Details")
        table.add_column("Node", style="cyan")
        table.add_column("Status")
        table.add_column("Duration", justify="right")
        table.add_column("In", justify="right")
        table.add_column("Out", justify="right")
        table.add_column("Retries", justify="right")
        table.add_column("Error", style="red", max_width=40)

        for node_id, nd in nodes.items():
            node_dur = nd.get("duration_seconds")
            dur_str = f"{node_dur:.2f}s" if node_dur is not None else "-"
            table.add_row(
                node_id,
                _status_style(nd.get("status", "")),
                dur_str,
                str(nd.get("input_records", 0)),
                str(nd.get("output_records", 0)),
                str(nd.get("retries_used", 0)),
                nd.get("error") or "",
            )

        console.print(table)


def _print_run_detail(run_data: dict[str, Any]) -> None:
    """Print detailed run status (for the ``status`` command)."""
    console.print()
    status_str = _status_style(run_data.get("status", ""))
    console.print(f"[bold]Run ID:[/bold]   {run_data.get('run_id', '-')}")
    console.print(f"[bold]Pipeline:[/bold] {run_data.get('pipeline_name', '-')}")
    console.print(f"[bold]Status:[/bold]   {status_str}")
    console.print(f"[bold]Created:[/bold]  {run_data.get('created_at', '-')}")

    started = run_data.get("started_at")
    if started:
        console.print(f"[bold]Started:[/bold]  {started}")

    completed = run_data.get("completed_at")
    if completed:
        console.print(f"[bold]Finished:[/bold] {completed}")

    duration = run_data.get("duration_seconds")
    if duration is not None:
        console.print(f"[bold]Duration:[/bold] {duration:.2f}s")

    summary = run_data.get("summary")
    if isinstance(summary, dict):
        console.print()
        console.print("[bold]Summary:[/bold]")
        for key, val in summary.items():
            console.print(f"  {key}: {val}")

    # Node details.
    nodes = run_data.get("nodes", {})
    if nodes:
        console.print()
        table = Table(title="Node Details")
        table.add_column("Node", style="cyan")
        table.add_column("Status")
        table.add_column("Duration", justify="right")
        table.add_column("In", justify="right")
        table.add_column("Out", justify="right")
        table.add_column("Retries", justify="right")
        table.add_column("Error", style="red", max_width=50)

        for node_id, nd in nodes.items():
            node_dur = nd.get("duration_seconds")
            dur_str = f"{node_dur:.2f}s" if node_dur is not None else "-"
            table.add_row(
                node_id,
                _status_style(nd.get("status", "")),
                dur_str,
                str(nd.get("input_records", 0)),
                str(nd.get("output_records", 0)),
                str(nd.get("retries_used", 0)),
                nd.get("error") or "",
            )

        console.print(table)
