from __future__ import annotations

from pyspark.sql import functions as F

from spillway import bronze, sources, state


def test_every_feed_lands_one_partition_per_month(lake, spark):
    with state.session(lake.data.state_db) as conn:
        rows = state.table_rows(conn, "bronze_partition")
    assert {(row["feed"], row["file_month"]) for row in rows} == {
        (feed, month)
        for feed in ("yellow", "green", "fhv", "fhvhv")
        for month in ("2019-01", "2019-02")
    }
    for row in rows:
        assert row["files"] == 1
        assert row["bytes"] > 0


def test_both_fhv_layouts_land_in_one_table(lake, spark):
    frame = bronze.read(spark, lake, "fhv")
    rows = frame.select("schema_variant").distinct().collect()
    variants = {row["schema_variant"] for row in rows}
    assert variants == {"fhv_v1", "fhv_v2"}
    assert frame.filter(F.col("dropoff_datetime").isNull()).count() == 0


def test_timestamps_are_parsed_not_strings(lake, spark):
    frame = bronze.read(spark, lake, "yellow")
    assert dict(frame.dtypes)["pickup_datetime"] == "timestamp"
    first = frame.filter(F.col("pickup_datetime").isNotNull()).first()
    assert first["pickup_datetime"].year == 2019 or first["pickup_datetime"].year == 2018


def test_a_broken_row_is_kept_and_flagged(lake, spark):
    frame = bronze.read(spark, lake, "yellow")
    corrupt = frame.filter(F.col("is_corrupt"))
    assert corrupt.count() == 2
    assert corrupt.first()["corrupt_text"].startswith("broken")
    with state.session(lake.data.state_db) as conn:
        rows = state.table_rows(conn, "bronze_partition")
    assert sum(row["corrupt_rows"] for row in rows if row["feed"] == "yellow") == 2


def test_rows_outside_their_file_month_are_counted(lake):
    with state.session(lake.data.state_db) as conn:
        rows = state.table_rows(conn, "bronze_partition")
    off_month = {
        (row["feed"], row["file_month"]): row["out_of_window_rows"]
        for row in rows
        if row["out_of_window_rows"]
    }
    assert off_month[("yellow", "2019-01")] >= 1


def test_partition_layout_is_one_directory_per_month(lake):
    partition = sources.bronze_partition(lake.data.bronze, "green", "2019-01")
    assert partition.is_dir()
    assert list(partition.glob("*.parquet"))


def test_rebuild_is_skipped_when_the_partition_exists(lake, spark):
    summary = bronze.build(lake, spark)
    assert summary["partitions"] == 0
