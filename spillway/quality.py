from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from . import gold, silver, state
from .config import Config

LOG = logging.getLogger(__name__)


class QualityError(RuntimeError):
    pass


@dataclass
class Check:
    layer: str
    name: str
    severity: str
    value: float
    threshold: float
    passed: bool
    detail: str = ""


def _rate(part: int, whole: int) -> float:
    return float(part) / float(whole) if whole else 0.0


def bronze_checks(config: Config) -> list[Check]:
    out: list[Check] = []
    with state.session(config.data.state_db) as conn:
        partitions = state.table_rows(conn, "bronze_partition")
        files = state.table_rows(conn, "source_file")
    ready = [row for row in files if row["status"] == "ready"]
    built = {(row["feed"], row["file_month"]) for row in partitions}
    missing = [
        (row["feed"], row["month"]) for row in ready if (row["feed"], row["month"]) not in built
    ]
    out.append(
        Check(
            "bronze",
            "every_ready_file_is_loaded",
            "error",
            float(len(missing)),
            0.0,
            not missing,
            ", ".join(f"{feed} {month}" for feed, month in missing[:5]),
        )
    )
    by_feed: dict[str, dict[str, int]] = {}
    for row in partitions:
        bucket = by_feed.setdefault(row["feed"], {"rows": 0, "corrupt": 0, "out": 0, "small": 0})
        bucket["rows"] += row["rows"] or 0
        bucket["corrupt"] += row["corrupt_rows"] or 0
        bucket["out"] += row["out_of_window_rows"] or 0
        if (row["rows"] or 0) < config.quality.min_rows_per_month:
            bucket["small"] += 1
    for feed, bucket in sorted(by_feed.items()):
        corrupt_rate = _rate(bucket["corrupt"], bucket["rows"])
        out.append(
            Check(
                "bronze",
                f"corrupt_rate_{feed}",
                "error",
                corrupt_rate,
                config.quality.max_corrupt_rate,
                corrupt_rate <= config.quality.max_corrupt_rate,
                f"{bucket['corrupt']} of {bucket['rows']} rows",
            )
        )
        window_rate = _rate(bucket["out"], bucket["rows"])
        out.append(
            Check(
                "bronze",
                f"out_of_window_rate_{feed}",
                "warning",
                window_rate,
                config.quality.max_out_of_window_rate,
                window_rate <= config.quality.max_out_of_window_rate,
                f"{bucket['out']} rows carry a pickup outside their file month",
            )
        )
        out.append(
            Check(
                "bronze",
                f"months_above_floor_{feed}",
                "error",
                float(bucket["small"]),
                0.0,
                bucket["small"] == 0,
                f"{bucket['small']} months below {config.quality.min_rows_per_month} rows",
            )
        )
    return out


def silver_checks(config: Config, spark: SparkSession) -> list[Check]:
    trips = silver.read(spark, config)
    stats = (
        trips.groupBy("feed")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(F.col("unknown_zone").cast("long")).alias("unknown"),
            F.sum(F.col("trip_id").isNull().cast("long")).alias("null_id"),
            F.sum(F.col("pickup_ts").isNull().cast("long")).alias("null_pickup"),
        )
        .collect()
    )
    out: list[Check] = []
    total_rows = 0
    for row in stats:
        total_rows += row["rows"]
        unknown_rate = _rate(row["unknown"], row["rows"])
        out.append(
            Check(
                "silver",
                f"unknown_zone_rate_{row['feed']}",
                "warning",
                unknown_rate,
                config.quality.max_unknown_zone_rate,
                unknown_rate <= config.quality.max_unknown_zone_rate,
                f"{row['unknown']} of {row['rows']} trips have no usable zone",
            )
        )
        out.append(
            Check(
                "silver",
                f"null_trip_id_{row['feed']}",
                "error",
                float(row["null_id"]),
                0.0,
                row["null_id"] == 0,
                "",
            )
        )
        null_rate = _rate(row["null_pickup"], row["rows"])
        out.append(
            Check(
                "silver",
                f"null_pickup_rate_{row['feed']}",
                "error",
                null_rate,
                config.quality.max_null_pickup_rate,
                null_rate <= config.quality.max_null_pickup_rate,
                "",
            )
        )
    biggest = (
        trips.groupBy("pickup_month")
        .agg(F.count(F.lit(1)).alias("rows"))
        .orderBy(F.desc("rows"))
        .first()
    )
    month = biggest["pickup_month"]
    scoped = trips.filter(F.col("pickup_month") == month)
    distinct_ids = scoped.select("trip_id").distinct().count()
    out.append(
        Check(
            "silver",
            "unique_trip_id_in_largest_month",
            "error",
            float(biggest["rows"] - distinct_ids),
            0.0,
            biggest["rows"] == distinct_ids,
            f"{month} holds {biggest['rows']} trips",
        )
    )
    with state.session(config.data.state_db) as conn:
        bronze_rows = sum(row["rows"] or 0 for row in state.table_rows(conn, "bronze_partition"))
        layers = {row["layer"]: row for row in state.table_rows(conn, "layer_run")}
    rejected_root = config.data.quarantine / "silver"
    quarantined = (
        spark.read.parquet(str(rejected_root)).count()
        if list(rejected_root.rglob("*.parquet"))
        else 0
    )
    for layer, observed in (("silver", total_rows), ("quarantine", quarantined)):
        recorded = layers.get(layer)
        expected = recorded["rows"] if recorded is not None else -1
        out.append(
            Check(
                "silver",
                f"{layer}_run_completed",
                "error",
                float(abs(observed - expected)),
                0.0,
                recorded is not None and expected == observed,
                f"recorded {expected}, found {observed} on disk",
            )
        )
    duplicates = bronze_rows - quarantined - total_rows
    out.append(
        Check(
            "silver",
            "row_reconciliation",
            "error",
            float(duplicates),
            0.0,
            duplicates >= 0,
            f"bronze {bronze_rows} = silver {total_rows} + quarantine {quarantined}"
            f" + duplicates {duplicates}",
        )
    )
    return out


def gold_checks(config: Config, spark: SparkSession) -> list[Check]:
    out: list[Check] = []
    trips = silver.read(spark, config).count()
    base = gold.read(spark, config, "od_month")
    mart_total = base.agg(F.sum("trips")).first()[0] or 0
    out.append(
        Check(
            "gold",
            "od_month_matches_silver",
            "error",
            float(abs(int(mart_total) - trips)),
            0.0,
            int(mart_total) == trips,
            f"od_month {int(mart_total)} against silver {trips}",
        )
    )
    for name in gold.MARTS:
        rows = gold.read(spark, config, name).count()
        out.append(
            Check("gold", f"{name}_not_empty", "error", float(rows), 1.0, rows > 0, "")
        )
    shares = (
        gold.read(spark, config, "feed_share")
        .groupBy("pickup_month")
        .agg(F.sum("trip_share").alias("total"))
        .select(F.max(F.abs(F.col("total") - F.lit(1.0))).alias("drift"))
        .first()["drift"]
    )
    drift = float(shares or 0.0)
    out.append(
        Check("gold", "feed_share_sums_to_one", "error", drift, 1e-6, drift <= 1e-6, "")
    )
    return out


def run(config: Config, spark: SparkSession, gate: bool = True) -> dict:
    started = time.perf_counter()
    checks = bronze_checks(config)
    checks.extend(silver_checks(config, spark))
    checks.extend(gold_checks(config, spark))
    with state.session(config.data.state_db) as conn:
        state.record_quality(
            conn,
            [
                {
                    "layer": check.layer,
                    "name": check.name,
                    "severity": check.severity,
                    "value": check.value,
                    "threshold": check.threshold,
                    "passed": int(check.passed),
                    "detail": check.detail,
                }
                for check in checks
            ],
        )
    failed = [check for check in checks if not check.passed]
    blocking = [check for check in failed if check.severity == config.quality.fail_on]
    LOG.info(
        "quality: %d checks, %d failed, %d blocking, %.1f s",
        len(checks),
        len(failed),
        len(blocking),
        time.perf_counter() - started,
    )
    for check in failed:
        LOG.warning(
            "quality: %s %s value %.6f threshold %.6f %s",
            check.severity,
            check.name,
            check.value,
            check.threshold,
            check.detail,
        )
    if gate and blocking:
        raise QualityError(
            f"{len(blocking)} blocking checks failed: "
            + ", ".join(check.name for check in blocking)
        )
    return {
        "checks": [asdict(check) for check in checks],
        "failed": len(failed),
        "blocking": len(blocking),
    }
