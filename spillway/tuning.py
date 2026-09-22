from __future__ import annotations

import json
import logging
import shutil
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import bronze, schemas, silver, sources, state
from .config import Config
from .metrics import Probe, StageMetrics, measure

LOG = logging.getLogger(__name__)
AQE = "spark.sql.adaptive.enabled"
BROADCAST = "spark.sql.autoBroadcastJoinThreshold"
SHUFFLE = "spark.sql.shuffle.partitions"


@dataclass
class Measurement:
    experiment: str
    variant: str
    rows: int
    metrics: StageMetrics
    detail: str = ""


@contextmanager
def conf(spark: SparkSession, values: dict[str, str]):
    previous = {}
    for key in values:
        try:
            previous[key] = spark.conf.get(key)
        except Exception:
            previous[key] = None
    for key, value in values.items():
        spark.conf.set(key, value)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)


def _landing_paths(config: Config, feed: str, months: tuple[str, ...]) -> list[Path]:
    return [
        sources.landing_path(config.data.landing, feed, month)
        for month in months
        if sources.landing_path(config.data.landing, feed, month).exists()
    ]


def _variant_for(config: Config, feed: str, month: str):
    with state.session(config.data.state_db) as conn:
        row = state.file_status(conn, feed, month)
    if row is None or not row["variant"]:
        return None
    return next(
        item for items in schemas.VARIANTS.values() for item in items if item.name == row["variant"]
    )


def gzip_parallelism(config: Config, spark: SparkSession, probe: Probe) -> list[Measurement]:
    feed = config.tuning.sample_feed
    months = config.tuning.sample_months
    paths = _landing_paths(config, feed, months)
    variant = _variant_for(config, feed, months[0])
    if not paths or variant is None:
        return []
    out = []
    frame = bronze.typed_frame(spark, variant, paths, config.bronze.timestamp_format)
    metrics, rows = measure(
        probe, lambda: frame.groupBy("file_month").count().collect()
    )
    out.append(
        Measurement(
            "gzip_parallelism",
            "gzip_csv",
            sum(row["count"] for row in rows),
            metrics,
            f"{len(paths)} gzip files",
        )
    )
    parquet = bronze.read(spark, config, feed).filter(F.col("file_month").isin(list(months)))
    metrics, rows = measure(probe, lambda: parquet.groupBy("file_month").count().collect())
    out.append(
        Measurement(
            "gzip_parallelism",
            "parquet_bronze",
            sum(row["count"] for row in rows),
            metrics,
            f"{len(paths)} months from parquet",
        )
    )
    return out


def schema_inference(config: Config, spark: SparkSession, probe: Probe) -> list[Measurement]:
    feed = config.tuning.sample_feed
    months = config.tuning.sample_months
    paths = _landing_paths(config, feed, months)
    variant = _variant_for(config, feed, months[0])
    if not paths or variant is None:
        return []
    uris = [path.resolve().as_uri() for path in paths]
    out = []
    explicit = (
        spark.read.options(**bronze.READ_OPTIONS).schema(variant.read_schema()).csv(uris)
    )
    metrics, rows = measure(probe, explicit.count)
    out.append(Measurement("schema_inference", "explicit_schema", rows, metrics, ""))
    inferred = spark.read.options(header="true", inferSchema="true").csv(uris)
    metrics, rows = measure(probe, inferred.count)
    out.append(Measurement("schema_inference", "inferred_schema", rows, metrics, ""))
    return out


def _sample_trips(config: Config, spark: SparkSession) -> DataFrame:
    return silver.read(spark, config).filter(
        F.col("pickup_month").isin(list(config.tuning.sample_months))
    )


def join_strategy(config: Config, spark: SparkSession, probe: Probe) -> list[Measurement]:
    trips = _sample_trips(config, spark).select("pu_location_id", "trip_seconds")
    zones = silver.zones(spark, config).select("location_id", "borough")
    out = []

    def query():
        return (
            trips.join(zones, trips.pu_location_id == zones.location_id, "inner")
            .groupBy("borough")
            .agg(F.count(F.lit(1)).alias("trips"))
            .collect()
        )

    with conf(spark, {BROADCAST: "-1"}):
        metrics, rows = measure(probe, query)
        out.append(
            Measurement(
                "join_strategy",
                "sort_merge",
                sum(row["trips"] for row in rows),
                metrics,
                "broadcast disabled",
            )
        )
    with conf(spark, {BROADCAST: str(32 * 1024 * 1024)}):
        metrics, rows = measure(probe, query)
        out.append(
            Measurement(
                "join_strategy",
                "broadcast",
                sum(row["trips"] for row in rows),
                metrics,
                "broadcast under 32 MB",
            )
        )
    return out


def skew_salting(config: Config, spark: SparkSession, probe: Probe) -> list[Measurement]:
    trips = (
        silver.read(spark, config)
        .filter(F.col("feed") == config.tuning.skew_feed)
        .filter(F.col("file_month").isin(list(config.tuning.skew_months)))
        .select("pu_location_id", "trip_seconds")
    )
    zones = silver.zones(spark, config).select("location_id", "borough")
    buckets = config.tuning.salt_buckets
    out = []
    def direct():
        return (
            trips.join(zones, trips.pu_location_id == zones.location_id, "left")
            .groupBy("borough")
            .agg(F.count(F.lit(1)).alias("trips"))
            .collect()
        )

    with conf(spark, {BROADCAST: "-1", AQE: "false"}):
        metrics, rows = measure(probe, direct)
        out.append(
            Measurement(
                "skew_salting",
                "direct_join",
                sum(row["trips"] for row in rows),
                metrics,
                "null and hot keys land on one task",
            )
        )
        salted_left = trips.withColumn(
            "join_key",
            F.when(
                F.col("pu_location_id").isNull(),
                -F.floor(F.rand(7) * buckets).cast("int") - 1,
            ).otherwise(F.col("pu_location_id")),
        )
        salted_right = zones.union(
            spark.range(1, buckets + 1).select(
                (-F.col("id")).cast("int").alias("location_id"),
                F.lit(None).cast("string").alias("borough"),
            )
        )

        def salted():
            return (
                salted_left.join(
                    salted_right, salted_left.join_key == salted_right.location_id, "left"
                )
                .groupBy("borough")
                .agg(F.count(F.lit(1)).alias("trips"))
                .collect()
            )

        metrics, rows = measure(probe, salted)
        out.append(
            Measurement(
                "skew_salting",
                "salted_join",
                sum(row["trips"] for row in rows),
                metrics,
                f"null keys spread over {buckets} buckets",
            )
        )
    return out


def adaptive_execution(config: Config, spark: SparkSession, probe: Probe) -> list[Measurement]:
    trips = _sample_trips(config, spark).select("pu_location_id", "pickup_hour", "trip_seconds")

    def query():
        return (
            trips.groupBy("pu_location_id", "pickup_hour")
            .agg(F.count(F.lit(1)).alias("trips"), F.avg("trip_seconds").alias("avg_seconds"))
            .collect()
        )

    out = []
    for variant, enabled in (("adaptive_off", "false"), ("adaptive_on", "true")):
        with conf(spark, {AQE: enabled, SHUFFLE: "200"}):
            metrics, rows = measure(probe, query)
            out.append(
                Measurement(
                    "adaptive_execution",
                    variant,
                    len(rows),
                    metrics,
                    "200 shuffle partitions requested",
                )
            )
    return out


def partition_pruning(config: Config, spark: SparkSession, probe: Probe) -> list[Measurement]:
    month = config.tuning.sample_months[0]
    trips = silver.read(spark, config)
    out = []

    def pruned():
        return trips.filter(F.col("pickup_month") == month).count()

    def scanned():
        return trips.filter(F.date_format("pickup_ts", "yyyy-MM") == month).count()

    metrics, rows = measure(probe, pruned)
    out.append(
        Measurement(
            "partition_pruning", "partition_filter", rows, metrics, f"pickup_month = {month}"
        )
    )
    metrics, rows = measure(probe, scanned)
    out.append(
        Measurement(
            "partition_pruning",
            "derived_filter",
            rows,
            metrics,
            "same month computed from the timestamp",
        )
    )
    return out


def file_compaction(config: Config, spark: SparkSession, probe: Probe) -> list[Measurement]:
    month = config.tuning.sample_months[0]
    source = silver.read(spark, config).filter(F.col("pickup_month") == month)
    scratch = config.data.root / "tuning"
    if scratch.exists():
        shutil.rmtree(scratch)
    many = scratch / "many_files"
    few = scratch / "few_files"
    source.repartition(200).write.mode("overwrite").parquet(str(many))
    source.coalesce(1).write.mode("overwrite").parquet(str(few))
    out = []
    for variant, path in (("many_small_files", many), ("compacted_files", few)):
        frame = spark.read.parquet(str(path))
        metrics, rows = measure(
            probe,
            lambda frame=frame: frame.groupBy("pu_location_id")
            .agg(F.count(F.lit(1)).alias("trips"))
            .collect(),
        )
        files = len(list(path.rglob("*.parquet")))
        size = sum(item.stat().st_size for item in path.rglob("*.parquet"))
        out.append(
            Measurement(
                "file_compaction",
                variant,
                sum(row["trips"] for row in rows),
                metrics,
                f"{files} files, {size / 1e6:.1f} MB on disk",
            )
        )
    shutil.rmtree(scratch, ignore_errors=True)
    return out


EXPERIMENTS = {
    "gzip_parallelism": gzip_parallelism,
    "schema_inference": schema_inference,
    "join_strategy": join_strategy,
    "skew_salting": skew_salting,
    "adaptive_execution": adaptive_execution,
    "partition_pruning": partition_pruning,
    "file_compaction": file_compaction,
}


def run(config: Config, spark: SparkSession, only: tuple[str, ...] | None = None) -> dict:
    probe = Probe(spark)
    if not probe.available():
        LOG.warning("tuning: the Spark UI is off, only wall clock will be recorded")
    wanted = only or config.tuning.experiments
    results: list[Measurement] = []
    for name in wanted:
        if name not in EXPERIMENTS:
            raise ValueError(f"unknown experiment {name}")
        LOG.info("tuning: running %s", name)
        measurements = EXPERIMENTS[name](config, spark, probe)
        if not measurements:
            LOG.warning("tuning: %s had no input and was skipped", name)
            continue
        counts = {item.rows for item in measurements}
        if len(counts) > 1:
            raise RuntimeError(f"{name} variants disagree on row counts: {counts}")
        results.extend(measurements)
        for item in measurements:
            LOG.info(
                "tuning: %s %s %.1fs tasks=%d shuffle_read=%.1fMB max_task=%dms",
                item.experiment,
                item.variant,
                item.metrics.wall_seconds,
                item.metrics.tasks,
                item.metrics.shuffle_read_bytes / 1e6,
                item.metrics.max_task_ms,
            )
    with state.session(config.data.state_db) as conn:
        for item in results:
            state.record_tuning(
                conn,
                experiment=item.experiment,
                variant=item.variant,
                wall_seconds=item.metrics.wall_seconds,
                tasks=item.metrics.tasks,
                executor_seconds=round(item.metrics.executor_seconds, 3),
                shuffle_read_bytes=item.metrics.shuffle_read_bytes,
                shuffle_write_bytes=item.metrics.shuffle_write_bytes,
                spill_memory_bytes=item.metrics.spill_memory_bytes,
                spill_disk_bytes=item.metrics.spill_disk_bytes,
                input_bytes=item.metrics.input_bytes,
                max_task_ms=item.metrics.max_task_ms,
                median_task_ms=item.metrics.median_task_ms,
                rows=item.rows,
                detail=item.detail,
            )
    config.data.reports.mkdir(parents=True, exist_ok=True)
    report = config.data.reports / "tuning.json"
    report.write_text(
        json.dumps([asdict(item) for item in results], indent=2, default=str), encoding="utf-8"
    )
    LOG.info("tuning done: %d measurements written to %s", len(results), report)
    return {"measurements": len(results), "report": str(report)}
