"""Tests for pipeline_engine.validation.dead_letter — dead letter queue."""

from __future__ import annotations

import pytest

from pipeline_engine.validation.dead_letter import DeadLetterQueue


class TestDeadLetterQueue:
    def test_add_and_retrieve(self):
        """Added entries appear in get_all() with correct fields."""
        dlq = DeadLetterQueue(max_size=100)
        dlq.add({"id": 1}, error="bad value", source="validation")
        dlq.add({"id": 2}, error="missing field", source="transform")

        assert dlq.size == 2
        entries = dlq.get_all()
        assert len(entries) == 2
        assert entries[0].record == {"id": 1}
        assert entries[0].error == "bad value"
        assert entries[0].source == "validation"
        assert entries[1].record == {"id": 2}

    def test_max_size_eviction(self):
        """When the queue exceeds max_size, the oldest entry is evicted."""
        dlq = DeadLetterQueue(max_size=3)
        for i in range(5):
            dlq.add({"id": i}, error=f"err_{i}")

        assert dlq.size == 3
        entries = dlq.get_all()
        # Only the last 3 should remain (ids 2, 3, 4)
        ids = [e.record["id"] for e in entries]
        assert ids == [2, 3, 4]

    def test_flush_clears_queue(self):
        """flush() returns all entries and leaves the queue empty."""
        dlq = DeadLetterQueue()
        dlq.add({"id": 1}, error="err1")
        dlq.add({"id": 2}, error="err2")

        flushed = dlq.flush()
        assert len(flushed) == 2
        assert dlq.size == 0
        assert dlq.get_all() == []

    def test_get_by_source(self):
        """get_by_source filters entries by their source field."""
        dlq = DeadLetterQueue()
        dlq.add({"a": 1}, error="e1", source="schema")
        dlq.add({"b": 2}, error="e2", source="transform")
        dlq.add({"c": 3}, error="e3", source="schema")

        schema_entries = dlq.get_by_source("schema")
        assert len(schema_entries) == 2
        transform_entries = dlq.get_by_source("transform")
        assert len(transform_entries) == 1

    def test_to_records(self):
        """to_records() serializes entries to plain dicts."""
        dlq = DeadLetterQueue()
        dlq.add({"x": 1}, error="err", source="test")

        records = dlq.to_records()
        assert len(records) == 1
        r = records[0]
        assert r["record"] == {"x": 1}
        assert r["error"] == "err"
        assert r["source"] == "test"
        assert "timestamp" in r

    def test_len_dunder(self):
        """__len__ returns the current queue size."""
        dlq = DeadLetterQueue()
        assert len(dlq) == 0
        dlq.add({"x": 1}, error="err")
        assert len(dlq) == 1

    def test_invalid_max_size_raises(self):
        """max_size < 1 raises ValueError."""
        with pytest.raises(ValueError, match="max_size must be >= 1"):
            DeadLetterQueue(max_size=0)
