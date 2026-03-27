"""Tests for pipeline_engine.transforms.filter — condition-based record filtering."""

from __future__ import annotations

import pytest

from pipeline_engine.transforms.filter import FilterTransform


class TestFilterTransform:
    def test_numeric_comparison_greater_than(self):
        """Filter with '>' keeps only records above the threshold."""
        ft = FilterTransform("age > 28")
        data = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35},
        ]
        result = ft.execute(data)
        assert len(result) == 2
        assert result[0]["name"] == "Alice"
        assert result[1]["name"] == "Charlie"

    def test_numeric_comparison_less_equal(self):
        """Filter with '<=' keeps records at or below the threshold."""
        ft = FilterTransform("salary <= 65000")
        data = [
            {"name": "A", "salary": 70000},
            {"name": "B", "salary": 60000},
            {"name": "C", "salary": 65000},
        ]
        result = ft.execute(data)
        assert len(result) == 2
        names = [r["name"] for r in result]
        assert "B" in names
        assert "C" in names

    def test_string_equality(self):
        """Filter with '==' on quoted string values."""
        ft = FilterTransform("status == 'active'")
        data = [
            {"id": 1, "status": "active"},
            {"id": 2, "status": "inactive"},
            {"id": 3, "status": "active"},
        ]
        result = ft.execute(data)
        assert len(result) == 2
        assert all(r["status"] == "active" for r in result)

    def test_string_inequality(self):
        """Filter with '!=' excludes matching records."""
        ft = FilterTransform("color != 'red'")
        data = [
            {"item": "A", "color": "red"},
            {"item": "B", "color": "blue"},
            {"item": "C", "color": "green"},
        ]
        result = ft.execute(data)
        assert len(result) == 2
        assert all(r["color"] != "red" for r in result)

    def test_in_list(self):
        """Filter with 'in [...]' keeps matching records."""
        ft = FilterTransform("category in ['A', 'C']")
        data = [
            {"id": 1, "category": "A"},
            {"id": 2, "category": "B"},
            {"id": 3, "category": "C"},
            {"id": 4, "category": "D"},
        ]
        result = ft.execute(data)
        assert len(result) == 2
        assert {r["category"] for r in result} == {"A", "C"}

    def test_regex_match(self):
        """Filter with '~' regex operator matches pattern."""
        ft = FilterTransform("name ~ '^J'")
        data = [
            {"name": "John"},
            {"name": "Jane"},
            {"name": "Alice"},
            {"name": "Jack"},
        ]
        result = ft.execute(data)
        assert len(result) == 3
        assert all(r["name"].startswith("J") for r in result)

    def test_missing_field_excluded(self):
        """Records missing the filter field are excluded."""
        ft = FilterTransform("score > 50")
        data = [
            {"name": "A", "score": 80},
            {"name": "B"},
            {"name": "C", "score": 30},
        ]
        result = ft.execute(data)
        assert len(result) == 1
        assert result[0]["name"] == "A"

    def test_invalid_condition_raises(self):
        """An unparseable condition raises ValueError."""
        with pytest.raises(ValueError, match="Unable to parse"):
            FilterTransform("this is not valid")

    def test_empty_input_returns_empty(self):
        """Filtering an empty list returns an empty list."""
        ft = FilterTransform("x > 0")
        assert ft.execute([]) == []
