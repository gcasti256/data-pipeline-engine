"""Tests for pipeline_engine.config — YAML config parsing and validation."""

from __future__ import annotations

import pytest
import yaml

from pipeline_engine.config.models import PipelineConfig
from pipeline_engine.config.parser import parse_config, validate_config

_VALID_CONFIG = {
    "name": "test_pipeline",
    "version": "1.0",
    "sources": {
        "orders_csv": {
            "type": "csv",
            "path": "data/orders.csv",
        }
    },
    "transforms": [
        {
            "name": "filter_active",
            "type": "filter",
            "input": "orders_csv",
            "condition": "status == 'active'",
        }
    ],
    "sinks": {
        "output_json": {
            "type": "json",
            "input": "filter_active",
            "path": "output/filtered.json",
        }
    },
}


def _write_yaml(tmp_path, data: dict, filename: str = "pipeline.yaml") -> str:
    """Write a dict as YAML to a temp file and return the path."""
    p = tmp_path / filename
    p.write_text(yaml.dump(data, default_flow_style=False))
    return str(p)


class TestParseConfig:
    def test_parse_valid_config(self, tmp_path):
        """A well-formed YAML file is parsed into a PipelineConfig."""
        path = _write_yaml(tmp_path, _VALID_CONFIG)
        config = parse_config(path)

        assert isinstance(config, PipelineConfig)
        assert config.name == "test_pipeline"
        assert config.version == "1.0"
        assert "orders_csv" in config.sources
        assert len(config.transforms) == 1
        assert config.transforms[0].name == "filter_active"
        assert "output_json" in config.sinks

    def test_parse_missing_file_raises(self):
        """Parsing a nonexistent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            parse_config("/nonexistent/path/config.yaml")

    def test_parse_invalid_yaml_raises(self, tmp_path):
        """Malformed YAML or missing required fields raises an error."""
        path = _write_yaml(tmp_path, {"name": "test"})  # missing sources/sinks
        with pytest.raises(Exception):
            parse_config(path)


class TestValidateConfig:
    def test_validate_clean_config(self, tmp_path):
        """A valid config produces no warnings."""
        config = PipelineConfig.model_validate(_VALID_CONFIG)
        warnings = validate_config(config)
        assert warnings == []

    def test_validate_unknown_connector_type(self):
        """An unknown connector type generates a warning."""
        data = dict(_VALID_CONFIG)
        data = {
            **_VALID_CONFIG,
            "sources": {
                "bad_source": {
                    "type": "mongodb",
                    "path": "fake",
                }
            },
        }
        config = PipelineConfig.model_validate(data)
        warnings = validate_config(config)
        assert any("unknown connector type" in w for w in warnings)

    def test_validate_dangling_input_reference(self):
        """A transform referencing a nonexistent input generates a warning."""
        data = {
            **_VALID_CONFIG,
            "transforms": [
                {
                    "name": "bad_ref",
                    "type": "filter",
                    "input": "nonexistent_source",
                    "condition": "x > 0",
                }
            ],
        }
        config = PipelineConfig.model_validate(data)
        warnings = validate_config(config)
        assert any("does not reference" in w for w in warnings)

    def test_validate_missing_filter_condition(self):
        """A filter transform without a condition generates a warning."""
        data = {
            **_VALID_CONFIG,
            "transforms": [
                {
                    "name": "no_condition",
                    "type": "filter",
                    "input": "orders_csv",
                    # condition is missing
                }
            ],
        }
        config = PipelineConfig.model_validate(data)
        warnings = validate_config(config)
        assert any("condition" in w for w in warnings)
