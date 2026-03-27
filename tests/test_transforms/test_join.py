"""Tests for pipeline_engine.transforms.join — hash-join operations."""

from __future__ import annotations

import pytest

from pipeline_engine.transforms.join import JoinTransform

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def left_data():
    return [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"},
    ]


@pytest.fixture
def right_data():
    return [
        {"id": 1, "score": 95},
        {"id": 2, "score": 87},
        {"id": 4, "score": 72},
    ]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestJoinTransform:
    def test_inner_join(self, left_data, right_data):
        """Inner join returns only matching records from both sides."""
        jt = JoinTransform(right_data=right_data, on="id", how="inner")
        result = jt.execute(left_data)

        assert len(result) == 2
        ids = {r["id"] for r in result}
        assert ids == {1, 2}
        # Check that both sides' columns are present
        assert all("name" in r for r in result)
        assert all("score" in r for r in result)

    def test_left_join(self, left_data, right_data):
        """Left join includes all left records, NULLs for unmatched right."""
        jt = JoinTransform(right_data=right_data, on="id", how="left")
        result = jt.execute(left_data)

        assert len(result) == 3
        charlie = next(r for r in result if r["id"] == 3)
        # Charlie has no matching right record, so score should be missing
        assert "score" not in charlie or charlie.get("score") is None

    def test_outer_join(self, left_data, right_data):
        """Outer join includes all records from both sides."""
        jt = JoinTransform(right_data=right_data, on="id", how="outer")
        result = jt.execute(left_data)

        # Left has 3 (ids 1,2,3), right has 3 (ids 1,2,4)
        # Inner: 2 matches (1,2), left-only: 1 (3), right-only: 1 (4)
        assert len(result) == 4
        result_ids = {r["id"] for r in result}
        assert result_ids == {1, 2, 3, 4}

    def test_right_join(self, left_data, right_data):
        """Right join includes all right records."""
        jt = JoinTransform(right_data=right_data, on="id", how="right")
        result = jt.execute(left_data)

        # Right has ids 1, 2, 4 -- id 4 has no left match
        assert len(result) == 3
        result_ids = {r["id"] for r in result}
        assert result_ids == {1, 2, 4}

    def test_multi_key_join(self):
        """Join on multiple key columns."""
        left = [
            {"dept": "eng", "level": "senior", "name": "Alice"},
            {"dept": "eng", "level": "junior", "name": "Bob"},
        ]
        right = [
            {"dept": "eng", "level": "senior", "bonus": 5000},
            {"dept": "eng", "level": "junior", "bonus": 2000},
        ]
        jt = JoinTransform(right_data=right, on=["dept", "level"], how="inner")
        result = jt.execute(left)

        assert len(result) == 2
        alice = next(r for r in result if r["name"] == "Alice")
        assert alice["bonus"] == 5000

    def test_invalid_join_type_raises(self):
        """An unsupported join type raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported join type"):
            JoinTransform(right_data=[], on="id", how="cross")

    def test_suffix_handling(self):
        """Overlapping non-key columns get suffixes applied."""
        left = [{"id": 1, "value": "left_val"}]
        right = [{"id": 1, "value": "right_val"}]
        jt = JoinTransform(
            right_data=right,
            on="id",
            how="inner",
            suffixes=("_l", "_r"),
        )
        result = jt.execute(left)

        assert len(result) == 1
        assert result[0]["value_l"] == "left_val"
        assert result[0]["value_r"] == "right_val"
