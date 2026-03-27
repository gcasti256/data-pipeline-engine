"""Tests for pipeline_engine.cli — Click CLI commands."""

from __future__ import annotations

import yaml
from click.testing import CliRunner

from pipeline_engine.cli import cli

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_VALID_CONFIG = {
    "name": "cli_test_pipeline",
    "version": "1.0",
    "sources": {
        "src": {
            "type": "csv",
            "path": "data/input.csv",
        }
    },
    "transforms": [
        {
            "name": "filter_step",
            "type": "filter",
            "input": "src",
            "condition": "value > 0",
        }
    ],
    "sinks": {
        "out": {
            "type": "json",
            "input": "filter_step",
            "path": "output/result.json",
        }
    },
}


def _write_config(tmp_path, data: dict | None = None) -> str:
    """Write pipeline config YAML to a temp file and return the path."""
    p = tmp_path / "pipeline.yaml"
    p.write_text(yaml.dump(data or _VALID_CONFIG, default_flow_style=False))
    return str(p)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCLI:
    def test_validate_command(self, tmp_path):
        """The 'validate' command parses a valid config and exits cleanly."""
        config_path = _write_config(tmp_path)
        runner = CliRunner()
        result = runner.invoke(cli, ["validate", "--config", config_path])

        assert result.exit_code == 0
        assert "cli_test_pipeline" in result.output

    def test_validate_command_with_warnings(self, tmp_path):
        """The 'validate' command reports warnings for problematic configs."""
        bad_config = {
            **_VALID_CONFIG,
            "transforms": [
                {
                    "name": "bad",
                    "type": "filter",
                    "input": "src",
                    # missing condition
                }
            ],
        }
        config_path = _write_config(tmp_path, bad_config)
        runner = CliRunner()
        result = runner.invoke(cli, ["validate", "--config", config_path])

        assert result.exit_code == 0
        assert "Warning" in result.output or "condition" in result.output

    def test_validate_command_bad_file(self, tmp_path):
        """The 'validate' command fails on a nonexistent file."""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["validate", "--config", str(tmp_path / "nofile.yaml")]
        )
        # Click's exists=True check causes a non-zero exit code
        assert result.exit_code != 0

    def test_list_command_empty(self, tmp_path):
        """The 'list' command runs with an empty database."""
        db_path = str(tmp_path / "empty.db")
        runner = CliRunner()
        result = runner.invoke(cli, ["list", "--db", db_path])

        assert result.exit_code == 0
        assert "No pipeline runs" in result.output or result.output.strip() != ""

    def test_version_flag(self):
        """--version prints the version and exits."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert "0.1.0" in result.output
