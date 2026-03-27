"""Tests for pipeline_engine.transforms.aggregate — group-by aggregations."""

from __future__ import annotations

import pytest

from pipeline_engine.transforms.aggregate import AggregateTransform

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestAggregateTransform:
    def test_sum_aggregation(self):
        """Sum aggregation groups correctly and computes totals."""
        agg = AggregateTransform(
            group_by=["city"],
            aggregations={"total_salary": "sum(salary)"},
        )
        data = [
            {"city": "NYC", "salary": 70000},
            {"city": "NYC", "salary": 80000},
            {"city": "LA", "salary": 60000},
        ]
        result = agg.execute(data)

        by_city = {r["city"]: r for r in result}
        assert by_city["NYC"]["total_salary"] == 150000
        assert by_city["LA"]["total_salary"] == 60000

    def test_multiple_aggregations(self):
        """Multiple aggregation functions in a single transform."""
        agg = AggregateTransform(
            group_by=["dept"],
            aggregations={
                "avg_salary": "avg(salary)",
                "max_salary": "max(salary)",
                "min_salary": "min(salary)",
                "headcount": "count()",
            },
        )
        data = [
            {"dept": "eng", "salary": 100},
            {"dept": "eng", "salary": 200},
            {"dept": "eng", "salary": 300},
            {"dept": "sales", "salary": 150},
        ]
        result = agg.execute(data)

        eng = next(r for r in result if r["dept"] == "eng")
        assert eng["avg_salary"] == 200.0
        assert eng["max_salary"] == 300
        assert eng["min_salary"] == 100
        assert eng["headcount"] == 3

        sales = next(r for r in result if r["dept"] == "sales")
        assert sales["headcount"] == 1

    def test_count_only(self):
        """count() without a column name counts rows per group."""
        agg = AggregateTransform(
            group_by=["status"],
            aggregations={"n": "count()"},
        )
        data = [
            {"status": "active"},
            {"status": "active"},
            {"status": "inactive"},
        ]
        result = agg.execute(data)

        by_status = {r["status"]: r for r in result}
        assert by_status["active"]["n"] == 2
        assert by_status["inactive"]["n"] == 1

    def test_first_and_last(self):
        """first() and last() return the correct records."""
        agg = AggregateTransform(
            group_by=["grp"],
            aggregations={
                "first_val": "first(val)",
                "last_val": "last(val)",
            },
        )
        data = [
            {"grp": "x", "val": "a"},
            {"grp": "x", "val": "b"},
            {"grp": "x", "val": "c"},
        ]
        result = agg.execute(data)
        assert result[0]["first_val"] == "a"
        assert result[0]["last_val"] == "c"

    def test_invalid_aggregation_expression_raises(self):
        """An invalid expression format raises ValueError."""
        with pytest.raises(ValueError, match="Invalid aggregation"):
            AggregateTransform(
                group_by=["x"],
                aggregations={"bad": "not_valid"},
            )

    def test_unsupported_function_raises(self):
        """An unsupported function name raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported aggregation"):
            AggregateTransform(
                group_by=["x"],
                aggregations={"bad": "median(val)"},
            )
