from __future__ import annotations

from pyspark.sql import functions as F

from lakeetl import gold, silver


def test_every_mart_is_written(lake, spark):
    for name in gold.MARTS:
        assert gold.read(spark, lake, name).count() > 0


def test_od_month_totals_match_silver(lake, spark):
    trips = silver.read(spark, lake).count()
    total = gold.read(spark, lake, "od_month").agg(F.sum("trips")).first()[0]
    assert int(total) == trips


def test_feed_share_sums_to_one_per_month(lake, spark):
    shares = (
        gold.read(spark, lake, "feed_share")
        .groupBy("pickup_month")
        .agg(F.sum("trip_share").alias("total"))
        .collect()
    )
    assert shares
    for row in shares:
        assert abs(row["total"] - 1.0) < 1e-9


def test_zone_names_are_joined_onto_the_grain(lake, spark):
    base = gold.read(spark, lake, "od_month")
    jfk = base.filter(F.col("pu_location_id") == 132)
    assert jfk.count() > 0
    assert {row["pu_zone"] for row in jfk.select("pu_zone").distinct().collect()} == {"JFK Airport"}
    assert jfk.filter(~F.col("pu_is_airport")).count() == 0
    assert base.filter(F.col("pu_zone").isNull() & F.col("pu_location_id").isNotNull()).count() == 0


def test_airport_flow_covers_both_directions(lake, spark):
    flow = gold.read(spark, lake, "airport_flow")
    assert {row["direction"] for row in flow.select("direction").distinct().collect()} == {
        "from_airport",
        "to_airport",
    }


def test_zone_pair_is_ranked_within_a_month(lake, spark):
    pairs = gold.read(spark, lake, "zone_pair")
    for month in [row["pickup_month"] for row in pairs.select("pickup_month").distinct().collect()]:
        scoped = pairs.filter(F.col("pickup_month") == month).orderBy("rank").collect()
        assert [row["rank"] for row in scoped] == list(range(1, len(scoped) + 1))
        assert [row["trips"] for row in scoped] == sorted(
            [row["trips"] for row in scoped], reverse=True
        )


def test_hourly_demand_hours_stay_in_range(lake, spark):
    hours = gold.read(spark, lake, "hourly_demand")
    assert hours.filter((F.col("pickup_hour") < 0) | (F.col("pickup_hour") > 23)).count() == 0
    assert hours.filter((F.col("pickup_dow") < 1) | (F.col("pickup_dow") > 7)).count() == 0
