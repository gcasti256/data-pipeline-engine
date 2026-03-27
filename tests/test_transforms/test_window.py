"""Tests for pipeline_engine.transforms.window — sliding window aggregations."""

from __future__ import annotations

import pytest

from pipeline_engine.transforms.window import WindowTransform

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestWindowTransform:
    def test_sliding_window_avg(self):
        """Sliding window average computes correctly for complete windows."""
        wt = WindowTransform(size=3, step=1, aggregation="avg", column="value")
        data = [
            {"value": 10},
            {"value": 20},
            {"value": 30},
            {"value": 40},
            {"value": 50},
        ]
        result = wt.execute(data)

        out_col = "value_window_avg"
        # First two records have incomplete windows -> None
        assert result[0][out_col] is None
        assert result[1][out_col] is None
        # Window [10, 20, 30] -> avg 20.0
        assert result[2][out_col] == 20.0
        # Window [20, 30, 40] -> avg 30.0
        assert result[3][out_col] == 30.0
        # Window [30, 40, 50] -> avg 40.0
        assert result[4][out_col] == 40.0

    def test_window_sum(self):
        """Sliding window sum aggregation."""
        wt = WindowTransform(size=2, step=1, aggregation="sum", column="x")
        data = [{"x": 1}, {"x": 2}, {"x": 3}, {"x": 4}]
        result = wt.execute(data)

        out_col = "x_window_sum"
        assert result[0][out_col] is None
        assert result[1][out_col] == 3   # 1 + 2
        assert result[2][out_col] == 5   # 2 + 3
        assert result[3][out_col] == 7   # 3 + 4

    def test_window_min_max(self):
        """Sliding window min and max aggregations."""
        wt_min = WindowTransform(size=3, step=1, aggregation="min", column="v")
        wt_max = WindowTransform(size=3, step=1, aggregation="max", column="v")
        data = [{"v": 5}, {"v": 1}, {"v": 3}, {"v": 8}]

        result_min = wt_min.execute(data)
        result_max = wt_max.execute(data)

        assert result_min[2]["v_window_min"] == 1   # min(5, 1, 3)
        assert result_max[2]["v_window_max"] == 5   # max(5, 1, 3)
        assert result_min[3]["v_window_min"] == 1   # min(1, 3, 8)
        assert result_max[3]["v_window_max"] == 8   # max(1, 3, 8)

    def test_window_with_step(self):
        """Non-overlapping windows with step > 1."""
        wt = WindowTransform(size=2, step=2, aggregation="sum", column="v")
        data = [{"v": 1}, {"v": 2}, {"v": 3}, {"v": 4}]
        result = wt.execute(data)

        out_col = "v_window_sum"
        # Window starting at 0: [1, 2] -> sum 3 at index 1
        assert result[1][out_col] == 3
        # Window starting at 2: [3, 4] -> sum 7 at index 3
        assert result[3][out_col] == 7
        # Indices 0 and 2 should be None (no window ends there)
        assert result[0][out_col] is None
        assert result[2][out_col] is None

    def test_window_size_validation(self):
        """Invalid window size raises ValueError."""
        with pytest.raises(ValueError, match="size must be >= 1"):
            WindowTransform(size=0, aggregation="avg", column="x")

    def test_unsupported_aggregation_raises(self):
        """An unsupported aggregation function raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported aggregation"):
            WindowTransform(size=3, aggregation="median", column="x")

    def test_column_required_for_non_count(self):
        """Non-count aggregations require a column argument."""
        with pytest.raises(ValueError, match="requires a column"):
            WindowTransform(size=3, aggregation="sum", column="")
