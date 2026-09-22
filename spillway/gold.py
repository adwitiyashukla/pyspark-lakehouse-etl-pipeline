from __future__ import annotations

import logging
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from . import silver, state
from .config import Config

LOG = logging.getLogger(__name__)
MARTS = ("od_month", "zone_month", "feed_share", "airport_flow", "zone_pair", "hourly_demand")


def od_month(trips: DataFrame, zone_frame: DataFrame) -> DataFrame:
    grouped = trips.groupBy(
        "pickup_month", "feed", "pu_location_id", "do_location_id"
    ).agg(
        F.count(F.lit(1)).alias("trips"),
        F.sum("trip_miles").alias("total_miles"),
        F.sum("trip_seconds").alias("total_seconds"),
        F.sum("total_amount").alias("total_revenue"),
        F.sum("tip_amount").alias("total_tips"),
        F.sum("fare_amount").alias("total_fare"),
        F.sum(F.col("shared_ride").cast("long")).alias("shared_trips"),
        F.sum(F.col("unknown_zone").cast("long")).alias("unknown_zone_trips"),
        F.sum(F.col("out_of_window").cast("long")).alias("out_of_window_trips"),
    )
    pickup = zone_frame.select(
        F.col("location_id").alias("pu_location_id"),
        F.col("borough").alias("pu_borough"),
        F.col("zone").alias("pu_zone"),
        F.col("is_airport").alias("pu_is_airport"),
    )
    dropoff = zone_frame.select(
        F.col("location_id").alias("do_location_id"),
        F.col("borough").alias("do_borough"),
        F.col("zone").alias("do_zone"),
        F.col("is_airport").alias("do_is_airport"),
    )
    return grouped.join(F.broadcast(pickup), "pu_location_id", "left").join(
        F.broadcast(dropoff), "do_location_id", "left"
    )


def zone_month(base: DataFrame, min_trips: int) -> DataFrame:
    grouped = base.groupBy("pickup_month", "feed", "pu_location_id", "pu_zone", "pu_borough").agg(
        F.sum("trips").alias("trips"),
        F.sum("total_miles").alias("total_miles"),
        F.sum("total_seconds").alias("total_seconds"),
        F.sum("total_revenue").alias("total_revenue"),
        F.sum("total_tips").alias("total_tips"),
        F.sum("total_fare").alias("total_fare"),
        F.sum("shared_trips").alias("shared_trips"),
    )
    return (
        grouped.filter(F.col("trips") >= min_trips)
        .withColumn("avg_miles", F.col("total_miles") / F.col("trips"))
        .withColumn("avg_minutes", F.col("total_seconds") / F.col("trips") / 60.0)
        .withColumn(
            "avg_speed_mph",
            F.when(
                F.col("total_seconds") > 0,
                F.col("total_miles") / (F.col("total_seconds") / 3600.0),
            ),
        )
        .withColumn(
            "tip_rate",
            F.when(F.col("total_fare") > 0, F.col("total_tips") / F.col("total_fare")),
        )
        .withColumn("revenue_per_trip", F.col("total_revenue") / F.col("trips"))
    )


def feed_share(base: DataFrame) -> DataFrame:
    by_feed = base.groupBy("pickup_month", "feed").agg(
        F.sum("trips").alias("trips"),
        F.sum("total_revenue").alias("total_revenue"),
        F.sum("total_miles").alias("total_miles"),
    )
    window = Window.partitionBy("pickup_month")
    return (
        by_feed.withColumn("month_trips", F.sum("trips").over(window))
        .withColumn("trip_share", F.col("trips") / F.col("month_trips"))
        .withColumn("avg_miles", F.col("total_miles") / F.col("trips"))
    )


def airport_flow(base: DataFrame) -> DataFrame:
    pickups = (
        base.filter(F.col("pu_is_airport"))
        .groupBy("pickup_month", "feed", F.col("pu_zone").alias("airport"))
        .agg(
            F.sum("trips").alias("trips"),
            F.sum("total_revenue").alias("total_revenue"),
            F.sum("total_seconds").alias("total_seconds"),
            F.sum("total_miles").alias("total_miles"),
        )
        .withColumn("direction", F.lit("from_airport"))
    )
    dropoffs = (
        base.filter(F.col("do_is_airport"))
        .groupBy("pickup_month", "feed", F.col("do_zone").alias("airport"))
        .agg(
            F.sum("trips").alias("trips"),
            F.sum("total_revenue").alias("total_revenue"),
            F.sum("total_seconds").alias("total_seconds"),
            F.sum("total_miles").alias("total_miles"),
        )
        .withColumn("direction", F.lit("to_airport"))
    )
    return (
        pickups.unionByName(dropoffs)
        .withColumn("avg_minutes", F.col("total_seconds") / F.col("trips") / 60.0)
        .withColumn("avg_miles", F.col("total_miles") / F.col("trips"))
        .withColumn("revenue_per_trip", F.col("total_revenue") / F.col("trips"))
    )


def zone_pair(base: DataFrame, top: int) -> DataFrame:
    pairs = base.groupBy(
        "pickup_month", "pu_zone", "pu_borough", "do_zone", "do_borough"
    ).agg(
        F.sum("trips").alias("trips"),
        F.sum("total_seconds").alias("total_seconds"),
        F.sum("total_miles").alias("total_miles"),
    )
    window = Window.partitionBy("pickup_month").orderBy(F.desc("trips"))
    return (
        pairs.withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") <= top)
        .withColumn("avg_minutes", F.col("total_seconds") / F.col("trips") / 60.0)
        .withColumn("avg_miles", F.col("total_miles") / F.col("trips"))
    )


def hourly_demand(trips: DataFrame) -> DataFrame:
    return (
        trips.groupBy("pickup_month", "feed", "pickup_dow", "pickup_hour")
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.sum("trip_miles").alias("total_miles"),
            F.sum("trip_seconds").alias("total_seconds"),
            F.sum("total_amount").alias("total_revenue"),
        )
        .withColumn("avg_minutes", F.col("total_seconds") / F.col("trips") / 60.0)
        .withColumn(
            "avg_speed_mph",
            F.when(
                F.col("total_seconds") > 0,
                F.col("total_miles") / (F.col("total_seconds") / 3600.0),
            ),
        )
    )


def build(config: Config, spark: SparkSession) -> dict:
    started = time.perf_counter()
    trips = silver.read(spark, config)
    zone_frame = silver.zones(spark, config)
    LOG.info("gold: building the origin destination grain")
    base_path = config.data.gold / "od_month"
    od_month(trips, zone_frame).coalesce(4).write.mode("overwrite").parquet(str(base_path))
    base = spark.read.parquet(str(base_path)).cache()
    counts = {"od_month": base.count()}
    LOG.info("gold: od_month has %d rows", counts["od_month"])
    for name, frame, partitions in (
        ("zone_month", zone_month(base, config.gold.min_zone_trips), 1),
        ("feed_share", feed_share(base), 1),
        ("airport_flow", airport_flow(base), 1),
        ("zone_pair", zone_pair(base, config.gold.top_pairs), 1),
    ):
        path = config.data.gold / name
        frame.coalesce(partitions).write.mode("overwrite").parquet(str(path))
        counts[name] = spark.read.parquet(str(path)).count()
        LOG.info("gold: %s has %d rows", name, counts[name])
    path = config.data.gold / "hourly_demand"
    hourly_demand(trips).coalesce(1).write.mode("overwrite").parquet(str(path))
    counts["hourly_demand"] = spark.read.parquet(str(path)).count()
    LOG.info("gold: hourly_demand has %d rows", counts["hourly_demand"])
    base.unpersist()
    elapsed = time.perf_counter() - started
    size = sum(item.stat().st_size for item in config.data.gold.rglob("*.parquet"))
    with state.session(config.data.state_db) as conn:
        state.record_layer(
            conn,
            layer="gold",
            rows=sum(counts.values()),
            files=len(list(config.data.gold.rglob("*.parquet"))),
            bytes=size,
            seconds=round(elapsed, 3),
            detail=", ".join(f"{name}={value}" for name, value in sorted(counts.items())),
        )
    LOG.info("gold done in %.1f s: %s", elapsed, counts)
    return {"counts": counts, "seconds": elapsed}


def read(spark: SparkSession, config: Config, name: str) -> DataFrame:
    return spark.read.parquet(str(config.data.gold / name))
