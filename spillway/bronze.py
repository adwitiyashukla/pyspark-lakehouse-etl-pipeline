from __future__ import annotations

import logging
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from . import schemas, sources, state
from .config import Config
from .schemas import CORRUPT_COLUMN, Variant

LOG = logging.getLogger(__name__)
MONTH_PATTERN = r"_tripdata_(\d{4}-\d{2})\.csv\.gz"
READ_OPTIONS = {
    "header": "true",
    "mode": "PERMISSIVE",
    "columnNameOfCorruptRecord": CORRUPT_COLUMN,
    "sep": ",",
    "quote": '"',
    "escape": '"',
    "multiLine": "false",
    "encoding": "UTF-8",
    "emptyValue": "",
    "nullValue": "",
}


def _as_uri(path: Path) -> str:
    return path.resolve().as_uri()


def typed_frame(spark: SparkSession, variant: Variant, paths: list[Path], fmt: str) -> DataFrame:
    frame = (
        spark.read.options(**READ_OPTIONS)
        .schema(variant.read_schema())
        .csv([_as_uri(path) for path in paths])
    )
    source = F.input_file_name()
    projection = []
    for column in variant.columns:
        expression = F.col(column.name)
        if isinstance(column.cast_type, T.TimestampType):
            expression = F.to_timestamp(expression, fmt)
        elif column.cast_type is not None:
            expression = expression.cast(column.cast_type)
        projection.append(expression.alias(column.name))
    projection.append(F.lit(variant.feed).alias("feed"))
    projection.append(F.regexp_extract(source, MONTH_PATTERN, 1).alias("file_month"))
    projection.append(F.lit(variant.name).alias("schema_variant"))
    projection.append(F.element_at(F.split(source, "/"), -1).alias("source_file"))
    projection.append(F.col(CORRUPT_COLUMN).isNotNull().alias("is_corrupt"))
    projection.append(F.col(CORRUPT_COLUMN).alias("corrupt_text"))
    return frame.select(*projection)


def _pending(config: Config, feeds: tuple[str, ...] | None, force: bool) -> dict[str, list]:
    groups: dict[str, list] = {}
    with state.session(config.data.state_db) as conn:
        for row in state.ready_files(conn):
            feed, month = row["feed"], row["month"]
            if feeds is not None and feed not in feeds:
                continue
            built = state.bronze_status(conn, feed, month)
            partition = sources.bronze_partition(config.data.bronze, feed, month)
            if not force and built is not None and partition.exists():
                continue
            path = Path(row["path"])
            if not path.exists():
                continue
            groups.setdefault(row["variant"], []).append((feed, month, path))
    return groups


def _partition_files(root: Path, feed: str, month: str) -> tuple[int, int]:
    partition = sources.bronze_partition(root, feed, month)
    if not partition.exists():
        return 0, 0
    files = [item for item in partition.glob("*.parquet") if item.is_file()]
    return len(files), sum(item.stat().st_size for item in files)


def build(
    config: Config,
    spark: SparkSession,
    feeds: tuple[str, ...] | None = None,
    force: bool = False,
) -> dict:
    groups = _pending(config, feeds, force)
    if not groups:
        LOG.info("bronze: nothing to build")
        return {"partitions": 0, "rows": 0}
    target = config.data.bronze
    target.mkdir(parents=True, exist_ok=True)
    summary = {"partitions": 0, "rows": 0, "corrupt": 0, "out_of_window": 0, "seconds": 0.0}
    for variant_name, items in sorted(groups.items()):
        variant = next(
            candidate
            for feed_variants in schemas.VARIANTS.values()
            for candidate in feed_variants
            if candidate.name == variant_name
        )
        paths = [path for _, _, path in items]
        LOG.info("bronze: %s, %d files, %.2f GB compressed", variant_name, len(paths),
                 sum(path.stat().st_size for path in paths) / 1e9)
        started = time.perf_counter()
        frame = typed_frame(spark, variant, paths, config.bronze.timestamp_format)
        if not config.bronze.keep_corrupt_rows:
            frame = frame.filter(~F.col("is_corrupt")).drop("corrupt_text")
        feed_root = sources.bronze_feed(target, variant.feed)
        frame.write.mode("overwrite").partitionBy("file_month").parquet(str(feed_root))
        elapsed = time.perf_counter() - started
        months = [month for _, month, _ in items]
        feed = items[0][0]
        counts = (
            spark.read.parquet(str(feed_root))
            .filter(F.col("file_month").isin(months))
            .groupBy("file_month")
            .agg(
                F.count(F.lit(1)).alias("rows"),
                F.sum(F.col("is_corrupt").cast("long")).alias("corrupt_rows"),
                F.sum(
                    (
                        F.col("pickup_datetime").isNull()
                        | (F.date_format("pickup_datetime", "yyyy-MM") != F.col("file_month"))
                    ).cast("long")
                ).alias("out_of_window_rows"),
            )
            .collect()
        )
        with state.session(config.data.state_db) as conn:
            for row in counts:
                files, size = _partition_files(target, feed, row["file_month"])
                state.record_bronze(
                    conn,
                    feed=feed,
                    file_month=row["file_month"],
                    variant=variant_name,
                    rows=int(row["rows"]),
                    corrupt_rows=int(row["corrupt_rows"] or 0),
                    out_of_window_rows=int(row["out_of_window_rows"] or 0),
                    files=files,
                    bytes=size,
                    seconds=round(elapsed / max(len(items), 1), 3),
                )
                summary["partitions"] += 1
                summary["rows"] += int(row["rows"])
                summary["corrupt"] += int(row["corrupt_rows"] or 0)
                summary["out_of_window"] += int(row["out_of_window_rows"] or 0)
        summary["seconds"] += elapsed
        LOG.info(
            "bronze: %s done in %.1fs, %d rows so far",
            variant_name,
            elapsed,
            summary["rows"],
        )
    with state.session(config.data.state_db) as conn:
        totals = state.totals(conn)
        state.record_layer(
            conn,
            layer="bronze",
            rows=totals["bronze_rows"],
            files=sum(
                _partition_files(target, feed, month)[0]
                for feed in config.data.feeds
                for month in sources.month_range(config.data.start_month, config.data.end_month)
            ),
            bytes=sum(item.stat().st_size for item in target.rglob("*.parquet")),
            seconds=round(summary["seconds"], 3),
            detail=(
                f"{totals['bronze_corrupt_rows']} corrupt rows, "
                f"{summary['rows']} rows in the last build"
            ),
        )
    LOG.info(
        "bronze done: %d partitions, %d rows, %d corrupt, %d outside their file month",
        summary["partitions"],
        summary["rows"],
        summary["corrupt"],
        summary["out_of_window"],
    )
    return summary


def read(spark: SparkSession, config: Config, feed: str) -> DataFrame:
    root = sources.bronze_feed(config.data.bronze, feed)
    return spark.read.option("basePath", str(root)).parquet(str(root))


def feed_available(config: Config, feed: str) -> bool:
    return sources.bronze_feed(config.data.bronze, feed).exists()

