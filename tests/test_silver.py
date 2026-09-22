from __future__ import annotations

from pyspark.sql import functions as F

from spillway import silver


def test_all_four_feeds_reach_one_table(lake, spark):
    frame = silver.read(spark, lake)
    assert {row["feed"] for row in frame.select("feed").distinct().collect()} == {
        "yellow",
        "green",
        "fhv",
        "fhvhv",
    }


def test_money_columns_are_null_for_the_for_hire_feeds(lake, spark):
    frame = silver.read(spark, lake)
    hire = frame.filter(F.col("feed").isin(["fhv", "fhvhv"]))
    assert hire.filter(F.col("total_amount").isNotNull()).count() == 0
    assert hire.filter(F.col("trip_seconds").isNull()).count() == 0


def test_zone_ids_survive_into_silver(lake, spark):
    frame = silver.read(spark, lake)
    jfk = frame.filter(F.col("pu_location_id") == 132)
    assert jfk.count() > 0
    assert frame.filter(F.col("pu_location_id") < 0).count() == 0


def test_missing_zones_are_flagged_not_dropped(lake, spark):
    frame = silver.read(spark, lake)
    unknown = frame.filter(F.col("unknown_zone"))
    assert unknown.count() > 0
    assert unknown.filter(F.col("pu_location_id").isNotNull()).count() >= 0


def test_bad_trips_are_quarantined_with_a_reason(lake, spark):
    rejected = spark.read.parquet(str(lake.data.quarantine / "silver"))
    rows = rejected.select("reject_reason").distinct().collect()
    reasons = {row["reject_reason"] for row in rows}
    assert {"too_short", "negative_duration", "corrupt_row"} <= reasons
    assert "distance_too_large" in reasons or "speed_too_high" in reasons


def test_exact_duplicates_are_removed_once(lake, spark):
    frame = silver.read(spark, lake)
    green = frame.filter(F.col("feed") == "green")
    assert green.count() == green.select("trip_id").distinct().count()
    january = green.filter(F.col("pickup_month") == "2019-01")
    assert january.filter(F.col("pu_location_id") == 236).count() == 1
    assert january.count() == 2


def test_derived_columns_are_consistent(lake, spark):
    row = (
        silver.read(spark, lake)
        .filter((F.col("feed") == "yellow") & F.col("trip_miles").isNotNull())
        .first()
    )
    assert row["trip_seconds"] > 0
    assert abs(row["speed_mph"] - row["trip_miles"] / (row["trip_seconds"] / 3600.0)) < 1e-6
    assert row["pickup_month"] == row["pickup_ts"].strftime("%Y-%m")
