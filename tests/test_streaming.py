from __future__ import annotations

import pytest

from spillway import streaming


@pytest.fixture(scope="module")
def produced(lake, spark):
    return streaming.produce(lake, spark, month="2019-01")


def test_producer_writes_one_file_per_batch_plus_the_late_file(lake, produced):
    layout = streaming.paths(lake)
    batches = sorted(item.name for item in layout["incoming"].glob("*.parquet"))
    assert len(batches) == lake.streaming.producer_files + 1
    assert "batch_late.parquet" in batches
    assert produced["rows"] > 0


def test_the_stream_reads_every_event_once(lake, spark, produced):
    summary = streaming.run(lake, spark, reset=True)
    assert summary["batches"] >= 1
    assert summary["input_rows"] == produced["rows"]
    assert summary["emitted_rows"] > 0


def test_a_second_run_reprocesses_nothing(lake, spark, produced):
    again = streaming.run(lake, spark, reset=False)
    assert again["input_rows"] == 0


def test_late_events_are_dropped_by_the_watermark(lake, spark):
    summary = streaming.run(lake, spark, reset=True)
    assert summary["dropped_late_rows"] > 0


def test_window_output_carries_start_and_end(lake, spark):
    layout = streaming.paths(lake)
    frame = spark.read.parquet(str(layout["output"]))
    row = frame.first()
    assert row["window_end"] > row["window_start"]
    assert row["trips"] >= 1
