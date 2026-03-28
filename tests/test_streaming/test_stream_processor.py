"""Tests for the stream processor module."""

from __future__ import annotations

import time
from unittest.mock import patch

import pytest

from pipeline_engine.streaming.stream_processor import (
    IdempotentWriter,
    StreamJoin,
    WatermarkState,
    WindowConfig,
    WindowedAggregator,
    WindowState,
    WindowType,
)


class TestWindowConfig:
    def test_tumbling_defaults(self):
        config = WindowConfig(type=WindowType.TUMBLING, size_seconds=60)
        assert config.size_seconds == 60
        assert config.slide_seconds is None

    def test_sliding_auto_slide(self):
        config = WindowConfig(type=WindowType.SLIDING, size_seconds=60)
        assert config.slide_seconds == 30.0

    def test_session_auto_gap(self):
        config = WindowConfig(type=WindowType.SESSION, size_seconds=120)
        assert config.gap_seconds == 120.0

    def test_custom_sliding(self):
        config = WindowConfig(
            type=WindowType.SLIDING,
            size_seconds=60,
            slide_seconds=15,
        )
        assert config.slide_seconds == 15


class TestWindowState:
    def test_record_count(self):
        state = WindowState(
            window_start=0,
            window_end=60,
            records=[{"a": 1}, {"a": 2}],
        )
        assert state.record_count == 2

    def test_is_expired(self):
        state = WindowState(window_start=0, window_end=60)
        assert not state.is_expired(30)
        assert state.is_expired(60)
        assert state.is_expired(100)


class TestWatermarkState:
    def test_advance(self):
        wm = WatermarkState(max_lateness_seconds=10)
        wm.advance(100)
        assert wm.current_watermark == 90

    def test_advance_monotonic(self):
        wm = WatermarkState(max_lateness_seconds=5)
        wm.advance(100)
        wm.advance(90)  # older event shouldn't move watermark back
        assert wm.current_watermark == 95

    def test_is_late(self):
        wm = WatermarkState(max_lateness_seconds=5)
        wm.advance(100)
        assert wm.is_late(90)
        assert not wm.is_late(96)

    def test_late_events_counter(self):
        wm = WatermarkState(max_lateness_seconds=5)
        assert wm.late_events_dropped == 0


class TestWindowedAggregator:
    def _make_records(self, count, base_time=1000):
        return [
            {
                "merchant_category": "food" if i % 2 == 0 else "retail",
                "amount": (i + 1) * 10,
                "timestamp": base_time + i,
            }
            for i in range(count)
        ]

    def test_tumbling_window_aggregation(self):
        config = WindowConfig(type=WindowType.TUMBLING, size_seconds=60)
        aggregator = WindowedAggregator(
            config=config,
            group_by=["merchant_category"],
            aggregations={
                "total_amount": "sum(amount)",
                "tx_count": "count(*)",
            },
            timestamp_field="timestamp",
        )
        # All records in same window — timestamps in the past so windows
        # expire immediately during process()
        records = self._make_records(4, base_time=1000)
        results = aggregator.process(records)
        results.extend(aggregator.flush())
        assert len(results) >= 1

        for result in results:
            assert "total_amount" in result
            assert "tx_count" in result
            assert result["tx_count"] > 0

    def test_aggregation_functions(self):
        config = WindowConfig(type=WindowType.TUMBLING, size_seconds=3600)
        aggregator = WindowedAggregator(
            config=config,
            aggregations={
                "total": "sum(value)",
                "average": "avg(value)",
                "minimum": "min(value)",
                "maximum": "max(value)",
                "count": "count(*)",
            },
            timestamp_field="ts",
        )

        records = [
            {"value": 10, "ts": 1000},
            {"value": 20, "ts": 1001},
            {"value": 30, "ts": 1002},
        ]
        results = aggregator.process(records)
        results.extend(aggregator.flush())

        assert len(results) == 1
        r = results[0]
        assert r["total"] == 60
        assert r["average"] == 20.0
        assert r["minimum"] == 10
        assert r["maximum"] == 30
        assert r["count"] == 3

    def test_group_by(self):
        config = WindowConfig(type=WindowType.TUMBLING, size_seconds=3600)
        aggregator = WindowedAggregator(
            config=config,
            group_by=["category"],
            aggregations={"total": "sum(amount)"},
            timestamp_field="ts",
        )

        records = [
            {"category": "A", "amount": 10, "ts": 1000},
            {"category": "B", "amount": 20, "ts": 1001},
            {"category": "A", "amount": 30, "ts": 1002},
        ]
        results = aggregator.process(records)
        results.extend(aggregator.flush())

        assert len(results) == 2
        totals = {r["category"]: r["total"] for r in results}
        assert totals["A"] == 40
        assert totals["B"] == 20

    def test_watermark_drops_late_events(self):
        wm = WatermarkState(max_lateness_seconds=5)
        wm.advance(200)  # watermark = 195

        config = WindowConfig(type=WindowType.TUMBLING, size_seconds=3600)
        aggregator = WindowedAggregator(
            config=config,
            aggregations={"count": "count(*)"},
            timestamp_field="ts",
            watermark=wm,
        )

        records = [
            {"ts": 100},   # late, should be dropped
            {"ts": 190},   # late, should be dropped
            {"ts": 196},   # not late
            {"ts": 210},   # not late
        ]
        results = aggregator.process(records)
        results.extend(aggregator.flush())

        total = sum(r["count"] for r in results)
        assert total == 2
        assert wm.late_events_dropped == 2

    def test_flush_empty(self):
        config = WindowConfig(type=WindowType.TUMBLING, size_seconds=60)
        aggregator = WindowedAggregator(config=config)
        results = aggregator.flush()
        assert results == []

    def test_sliding_window(self):
        config = WindowConfig(
            type=WindowType.SLIDING,
            size_seconds=10,
            slide_seconds=5,
        )
        aggregator = WindowedAggregator(
            config=config,
            aggregations={"count": "count(*)"},
            timestamp_field="ts",
        )
        records = [{"ts": 3}, {"ts": 7}]
        results = aggregator.process(records)
        results.extend(aggregator.flush())
        # Sliding windows may put records in multiple windows
        assert len(results) >= 1

    def test_session_window(self):
        config = WindowConfig(
            type=WindowType.SESSION,
            size_seconds=10,
            gap_seconds=5,
        )
        aggregator = WindowedAggregator(
            config=config,
            aggregations={"count": "count(*)"},
            timestamp_field="ts",
        )
        # Two sessions separated by gap > 5s
        records = [
            {"ts": 1},
            {"ts": 2},
            {"ts": 3},
            {"ts": 100},  # new session
            {"ts": 101},
        ]
        results = aggregator.process(records)
        results.extend(aggregator.flush())
        assert len(results) == 2


class TestStreamJoin:
    def test_basic_join(self):
        join = StreamJoin(left_key="id", right_key="id")
        join.add_left([{"id": "1", "name": "Alice"}])
        join.add_right([{"id": "1", "age": 30}])
        results = join.emit()

        assert len(results) == 1
        assert results[0]["name"] == "Alice"
        assert results[0]["age"] == 30

    def test_no_match(self):
        join = StreamJoin(left_key="id", right_key="id")
        join.add_left([{"id": "1", "name": "Alice"}])
        join.add_right([{"id": "2", "name": "Bob"}])
        results = join.emit()
        assert len(results) == 0

    def test_multiple_matches(self):
        join = StreamJoin(left_key="id", right_key="id")
        join.add_left([
            {"id": "1", "event": "a"},
            {"id": "1", "event": "b"},
        ])
        join.add_right([{"id": "1", "status": "ok"}])
        results = join.emit()
        assert len(results) == 2

    def test_buffer_sizes(self):
        join = StreamJoin(left_key="id", right_key="id")
        join.add_left([{"id": str(i)} for i in range(5)])
        join.add_right([{"id": str(i)} for i in range(3)])
        assert join.left_buffer_size == 5
        assert join.right_buffer_size == 3

    def test_expired_records_evicted(self):
        join = StreamJoin(left_key="id", right_key="id", window_seconds=0.01)
        join.add_left([{"id": "1", "val": "old"}])
        import time
        time.sleep(0.02)
        join.add_right([{"id": "1", "val": "new"}])
        results = join.emit()
        # Left side should have been evicted
        assert len(results) == 0

    def test_repr(self):
        join = StreamJoin(left_key="a", right_key="b", window_seconds=60)
        r = repr(join)
        assert "a" in r
        assert "b" in r


class TestIdempotentWriter:
    def test_filter_duplicates(self):
        writer = IdempotentWriter(id_field="id")
        records = [
            {"id": "1", "val": "a"},
            {"id": "2", "val": "b"},
            {"id": "1", "val": "c"},  # duplicate
        ]
        unique = writer.filter_duplicates(records)
        assert len(unique) == 2
        assert writer.tracked_count == 2

    def test_empty_id_skipped(self):
        writer = IdempotentWriter(id_field="id")
        records = [
            {"val": "no id"},
            {"id": "", "val": "empty id"},
            {"id": "1", "val": "ok"},
        ]
        unique = writer.filter_duplicates(records)
        assert len(unique) == 1

    def test_max_tracked_eviction(self):
        writer = IdempotentWriter(id_field="id", max_tracked=3)
        records = [{"id": str(i)} for i in range(5)]
        writer.filter_duplicates(records)
        assert writer.tracked_count == 3

    def test_repr(self):
        writer = IdempotentWriter()
        assert "IdempotentWriter" in repr(writer)

    def test_cross_batch_dedup(self):
        writer = IdempotentWriter(id_field="id")
        batch1 = [{"id": "1"}, {"id": "2"}]
        batch2 = [{"id": "2"}, {"id": "3"}]
        unique1 = writer.filter_duplicates(batch1)
        unique2 = writer.filter_duplicates(batch2)
        assert len(unique1) == 2
        assert len(unique2) == 1
        assert unique2[0]["id"] == "3"
