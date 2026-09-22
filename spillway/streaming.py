from __future__ import annotations

import logging
import os
import shutil
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from . import silver, state
from .config import Config

LOG = logging.getLogger(__name__)
EVENT_SCHEMA = T.StructType(
    [
        T.StructField("trip_id", T.LongType(), True),
        T.StructField("feed", T.StringType(), True),
        T.StructField("pickup_ts", T.TimestampType(), True),
        T.StructField("pu_location_id", T.IntegerType(), True),
        T.StructField("trip_miles", T.DoubleType(), True),
        T.StructField("trip_seconds", T.IntegerType(), True),
    ]
)


def paths(config: Config) -> dict[str, Path]:
    root = config.data.stream
    return {
        "incoming": root / "incoming",
        "checkpoint": root / "checkpoint",
        "output": root / "windows",
    }


def _flatten(frame, target: Path) -> None:
    staging = target.parent / f"_tmp_{target.stem}"
    shutil.rmtree(staging, ignore_errors=True)
    frame.coalesce(1).write.mode("overwrite").parquet(str(staging))
    parts = sorted(staging.glob("part-*.parquet"))
    if not parts:
        shutil.rmtree(staging, ignore_errors=True)
        return
    parts[0].replace(target)
    shutil.rmtree(staging, ignore_errors=True)


def _order_arrivals(folder: Path) -> None:
    files = sorted(folder.glob("*.parquet"))
    files.sort(key=lambda item: (item.stem == "batch_late", item.stem))
    stamp = time.time() - len(files) * 10
    for index, item in enumerate(files):
        moment = stamp + index * 10
        os.utime(item, (moment, moment))


def produce(config: Config, spark: SparkSession, month: str | None = None) -> dict:
    layout = paths(config)
    if layout["incoming"].exists():
        shutil.rmtree(layout["incoming"])
    layout["incoming"].mkdir(parents=True, exist_ok=True)
    trips = silver.read(spark, config)
    month = month or config.tuning.sample_months[0]
    selected = (
        trips.filter(F.col("pickup_month") == month)
        .select(*[field.name for field in EVENT_SCHEMA.fields])
        .limit(config.streaming.producer_rows)
        .orderBy("pickup_ts")
    )
    batches = config.streaming.producer_files
    frame = selected.withColumn(
        "batch", F.ntile(batches).over(Window.orderBy("pickup_ts"))
    ).cache()
    written = 0
    for index in range(1, batches + 1):
        part = frame.filter(F.col("batch") == index).drop("batch")
        _flatten(part, layout["incoming"] / f"batch_{index:03d}.parquet")
        written += 1
    late = None
    if config.streaming.late_rows:
        late = (
            frame.filter(F.col("batch") == 1)
            .drop("batch")
            .limit(config.streaming.late_rows)
            .withColumn(
                "pickup_ts",
                F.col("pickup_ts") - F.expr(f"INTERVAL {config.streaming.late_offset_hours} HOURS"),
            )
            .withColumn("trip_id", F.xxhash64(F.concat_ws("|", F.col("trip_id"), F.lit("late"))))
        )
        _flatten(late, layout["incoming"] / "batch_late.parquet")
        written += 1
    _order_arrivals(layout["incoming"])
    rows = spark.read.schema(EVENT_SCHEMA).parquet(str(layout["incoming"])).count()
    frame.unpersist()
    LOG.info("streaming: produced %d files holding %d events from %s", written, rows, month)
    return {"files": written, "rows": rows, "month": month}


def run(config: Config, spark: SparkSession, reset: bool = False) -> dict:
    layout = paths(config)
    if reset:
        for key in ("checkpoint", "output"):
            shutil.rmtree(layout[key], ignore_errors=True)
    if not layout["incoming"].exists():
        raise RuntimeError("no streaming input, run the producer first")
    source = (
        spark.readStream.schema(EVENT_SCHEMA)
        .option("maxFilesPerTrigger", config.streaming.files_per_batch)
        .parquet(str(layout["incoming"]))
    )
    watermarked = source.withWatermark("pickup_ts", f"{config.streaming.watermark_minutes} minutes")
    windows = (
        watermarked.groupBy(
            F.window("pickup_ts", f"{config.streaming.window_minutes} minutes").alias("slot"),
            F.col("pu_location_id"),
        )
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.sum("trip_miles").alias("total_miles"),
            F.avg("trip_seconds").alias("avg_seconds"),
        )
        .select(
            F.col("slot.start").alias("window_start"),
            F.col("slot.end").alias("window_end"),
            "pu_location_id",
            "trips",
            "total_miles",
            "avg_seconds",
        )
    )
    query = (
        windows.writeStream.format("parquet")
        .option("path", str(layout["output"]))
        .option("checkpointLocation", str(layout["checkpoint"]))
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    progress = list(query.recentProgress)
    summary = {
        "batches": len(progress),
        "input_rows": sum(int(item.get("numInputRows", 0)) for item in progress),
        "dropped_late_rows": 0,
        "state_rows": 0,
        "seconds": round(sum(float(item.get("batchDuration", 0)) for item in progress) / 1000.0, 3),
    }
    for item in progress:
        for operator in item.get("stateOperators", []):
            summary["dropped_late_rows"] += int(operator.get("numRowsDroppedByWatermark", 0))
            summary["state_rows"] = max(
                summary["state_rows"], int(operator.get("numRowsTotal", 0))
            )
    emitted = 0
    if layout["output"].exists():
        emitted = spark.read.parquet(str(layout["output"])).count()
    summary["emitted_rows"] = emitted
    with state.session(config.data.state_db) as conn:
        state.record_layer(
            conn,
            layer="streaming",
            rows=emitted,
            files=len(list(layout["output"].rglob("*.parquet"))),
            bytes=sum(item.stat().st_size for item in layout["output"].rglob("*.parquet")),
            seconds=summary["seconds"],
            detail=(
                f"{summary['batches']} batches, {summary['input_rows']} events, "
                f"{summary['dropped_late_rows']} dropped by the watermark"
            ),
        )
    LOG.info(
        "streaming: %d batches, %d events in, %d window rows out, %d dropped as late",
        summary["batches"],
        summary["input_rows"],
        emitted,
        summary["dropped_late_rows"],
    )
    return summary
