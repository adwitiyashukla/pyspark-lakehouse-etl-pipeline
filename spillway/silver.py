from __future__ import annotations

import logging
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from . import bronze, state
from .config import Config

LOG = logging.getLogger(__name__)
AIRPORT_ZONES = ("JFK Airport", "LaGuardia Airport", "Newark Airport")
ZONE_SCHEMA = T.StructType(
    [
        T.StructField("LocationID", T.IntegerType(), True),
        T.StructField("Borough", T.StringType(), True),
        T.StructField("Zone", T.StringType(), True),
    ]
)
MONEY_FEEDS = ("yellow", "green")
UNKNOWN_ZONE_IDS = (264, 265)
SILVER_COLUMNS = (
    "trip_id",
    "feed",
    "pickup_ts",
    "dropoff_ts",
    "pickup_date",
    "pickup_hour",
    "pickup_dow",
    "pu_location_id",
    "do_location_id",
    "passenger_count",
    "trip_miles",
    "trip_seconds",
    "speed_mph",
    "fare_amount",
    "tip_amount",
    "tolls_amount",
    "surcharges",
    "total_amount",
    "payment_type",
    "vendor_id",
    "license_num",
    "base_num",
    "shared_ride",
    "schema_variant",
    "file_month",
    "unknown_zone",
    "out_of_window",
    "zero_distance",
    "pickup_month",
)


def zones(spark: SparkSession, config: Config) -> DataFrame:
    path = config.data.reference / "taxi_zone_lookup.csv"
    frame = spark.read.options(header="true").schema(ZONE_SCHEMA).csv(path.resolve().as_uri())
    return frame.select(
        F.col("LocationID").alias("location_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone"),
        F.col("Zone").isin(list(AIRPORT_ZONES)).alias("is_airport"),
    )


def _missing(name: str, dtype: T.DataType):
    return F.lit(None).cast(dtype).alias(name)


def _harmonize(frame: DataFrame, feed: str) -> DataFrame:
    columns = set(frame.columns)
    double = T.DoubleType()
    string = T.StringType()
    integer = T.IntegerType()
    has_money = feed in MONEY_FEEDS
    surcharges = (
        F.coalesce(F.col("extra"), F.lit(0.0))
        + F.coalesce(F.col("mta_tax"), F.lit(0.0))
        + F.coalesce(F.col("improvement_surcharge"), F.lit(0.0))
        + F.coalesce(F.col("congestion_surcharge"), F.lit(0.0))
        if has_money
        else F.lit(None).cast(double)
    )
    return frame.select(
        F.col("pickup_datetime").alias("pickup_ts"),
        F.col("dropoff_datetime").alias("dropoff_ts"),
        F.col("pu_location_id"),
        F.col("do_location_id"),
        (F.col("passenger_count") if "passenger_count" in columns else _missing("x", double)).alias(
            "passenger_count"
        ),
        (F.col("trip_distance") if "trip_distance" in columns else _missing("x", double)).alias(
            "trip_miles"
        ),
        (F.col("fare_amount") if has_money else _missing("x", double)).alias("fare_amount"),
        (F.col("tip_amount") if has_money else _missing("x", double)).alias("tip_amount"),
        (F.col("tolls_amount") if has_money else _missing("x", double)).alias("tolls_amount"),
        surcharges.alias("surcharges"),
        (F.col("total_amount") if has_money else _missing("x", double)).alias("total_amount"),
        (F.col("payment_type") if has_money else _missing("x", integer)).alias("payment_type"),
        (F.col("vendor_id") if "vendor_id" in columns else _missing("x", integer)).alias(
            "vendor_id"
        ),
        (
            F.col("hvfhs_license_num") if "hvfhs_license_num" in columns else _missing("x", string)
        ).alias("license_num"),
        (
            F.col("dispatching_base_num")
            if "dispatching_base_num" in columns
            else _missing("x", string)
        ).alias("base_num"),
        (
            F.col("sr_flag").isin("1", "Y")
            if "sr_flag" in columns
            else F.lit(None).cast(T.BooleanType())
        ).alias("shared_ride"),
        F.col("feed"),
        F.col("file_month"),
        F.col("schema_variant"),
        F.col("is_corrupt"),
    )


def _enrich(frame: DataFrame, config: Config) -> DataFrame:
    seconds = F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")
    enriched = frame.withColumn("trip_seconds", seconds.cast(T.IntegerType()))
    enriched = enriched.withColumn(
        "speed_mph",
        F.when(
            (F.col("trip_seconds") > 0) & F.col("trip_miles").isNotNull(),
            F.col("trip_miles") / (F.col("trip_seconds") / 3600.0),
        ),
    )
    enriched = enriched.withColumn(
        "trip_id",
        F.xxhash64(
            F.concat_ws(
                "|",
                F.col("feed"),
                F.coalesce(F.col("pickup_ts").cast("string"), F.lit("")),
                F.coalesce(F.col("dropoff_ts").cast("string"), F.lit("")),
                F.coalesce(F.col("pu_location_id").cast("string"), F.lit("")),
                F.coalesce(F.col("do_location_id").cast("string"), F.lit("")),
                F.coalesce(F.col("total_amount").cast("string"), F.lit("")),
                F.coalesce(F.col("trip_miles").cast("string"), F.lit("")),
                F.coalesce(F.col("base_num"), F.lit("")),
            )
        ),
    )
    return (
        enriched.withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn("pickup_month", F.date_format("pickup_ts", "yyyy-MM"))
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        .withColumn("pickup_dow", F.dayofweek("pickup_ts"))
        .withColumn(
            "unknown_zone",
            F.col("pu_location_id").isNull()
            | F.col("do_location_id").isNull()
            | F.col("pu_location_id").isin(list(UNKNOWN_ZONE_IDS))
            | F.col("do_location_id").isin(list(UNKNOWN_ZONE_IDS)),
        )
        .withColumn("out_of_window", F.col("pickup_month") != F.col("file_month"))
        .withColumn(
            "zero_distance",
            F.col("trip_miles").isNotNull() & (F.col("trip_miles") <= 0),
        )
    )


def reject_reason(config: Config):
    silver = config.silver
    return (
        F.when(F.col("is_corrupt"), F.lit("corrupt_row"))
        .when(F.col("pickup_ts").isNull(), F.lit("null_pickup"))
        .when(F.col("dropoff_ts").isNull(), F.lit("null_dropoff"))
        .when(F.col("trip_seconds") < 0, F.lit("negative_duration"))
        .when(F.col("trip_seconds") < silver.min_trip_seconds, F.lit("too_short"))
        .when(F.col("trip_seconds") > silver.max_trip_seconds, F.lit("too_long"))
        .when(F.col("trip_miles") > silver.max_trip_miles, F.lit("distance_too_large"))
        .when(F.col("speed_mph") > silver.max_speed_mph, F.lit("speed_too_high"))
        .when(F.col("total_amount") > silver.max_fare, F.lit("fare_too_large"))
        .otherwise(F.lit(None))
    )


def build(config: Config, spark: SparkSession, months: tuple[str, ...] | None = None) -> dict:
    started = time.perf_counter()
    frames = []
    for feed in config.data.feeds:
        if not bronze.feed_available(config, feed):
            continue
        part = bronze.read(spark, config, feed)
        if months:
            part = part.filter(F.col("file_month").isin(list(months)))
        frames.append(_harmonize(part, feed))
    if not frames:
        raise RuntimeError("no bronze data found, run the bronze stage first")
    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame)
    enriched = _enrich(combined, config)
    enriched = enriched.withColumn("reject_reason", reject_reason(config))
    rejected = enriched.filter(F.col("reject_reason").isNotNull())
    accepted = enriched.filter(F.col("reject_reason").isNull()).select(*SILVER_COLUMNS)
    accepted = accepted.repartition("pickup_month")
    if config.silver.dedup:
        accepted = accepted.dropDuplicates(["pickup_month", "trip_id"])
    target = config.data.silver
    LOG.info("silver: writing accepted trips")
    accepted.write.mode("overwrite").partitionBy("pickup_month").parquet(str(target))
    LOG.info("silver: writing rejected trips")
    (
        rejected.select(
            "feed",
            "file_month",
            "schema_variant",
            "reject_reason",
            "pickup_ts",
            "dropoff_ts",
            "pu_location_id",
            "do_location_id",
            "trip_miles",
            "trip_seconds",
            "total_amount",
        )
        .repartition("reject_reason")
        .write.mode("overwrite")
        .partitionBy("reject_reason")
        .parquet(str(config.data.quarantine / "silver"))
    )
    elapsed = time.perf_counter() - started
    kept = spark.read.parquet(str(target)).count()
    dropped = spark.read.parquet(str(config.data.quarantine / "silver")).count()
    size = sum(item.stat().st_size for item in target.rglob("*.parquet"))
    files = len(list(target.rglob("*.parquet")))
    rejected_root = config.data.quarantine / "silver"
    with state.session(config.data.state_db) as conn:
        state.record_layer(
            conn,
            layer="silver",
            rows=kept,
            files=files,
            bytes=size,
            seconds=round(elapsed, 3),
            detail=f"{dropped} quarantined",
        )
        state.record_layer(
            conn,
            layer="quarantine",
            rows=dropped,
            files=len(list(rejected_root.rglob("*.parquet"))),
            bytes=sum(item.stat().st_size for item in rejected_root.rglob("*.parquet")),
            seconds=round(elapsed, 3),
            detail="trips rejected by silver rules",
        )
    LOG.info(
        "silver done: %d trips kept, %d quarantined, %d files, %.1f s",
        kept,
        dropped,
        files,
        elapsed,
    )
    return {"rows": kept, "quarantined": dropped, "files": files, "seconds": elapsed}


def read(spark: SparkSession, config: Config) -> DataFrame:
    return spark.read.option("basePath", str(config.data.silver)).parquet(str(config.data.silver))
