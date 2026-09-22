from __future__ import annotations

import os
import platform
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from .config import Config

SUBMIT_ARGS = "PYSPARK_SUBMIT_ARGS"


class SparkStartupError(RuntimeError):
    pass


def check_windows_hadoop() -> None:
    if platform.system() != "Windows":
        return
    home = os.environ.get("HADOOP_HOME", "")
    if not home or not (Path(home) / "bin" / "winutils.exe").exists():
        raise SparkStartupError(
            "Spark needs winutils.exe and hadoop.dll on Windows. Set HADOOP_HOME to a folder "
            "whose bin holds both, then reopen the terminal."
        )


def _bytes(megabytes: int) -> int:
    return megabytes * 1024 * 1024


def settings(config: Config, overrides: dict[str, str] | None = None) -> dict[str, str]:
    spark = config.spark
    values = {
        "spark.sql.shuffle.partitions": str(spark.shuffle_partitions),
        "spark.sql.adaptive.enabled": str(spark.adaptive).lower(),
        "spark.sql.adaptive.skewJoin.enabled": str(spark.adaptive_skew_join).lower(),
        "spark.sql.adaptive.coalescePartitions.enabled": str(spark.adaptive).lower(),
        "spark.sql.autoBroadcastJoinThreshold": (
            "-1" if spark.broadcast_threshold_mb < 0 else str(_bytes(spark.broadcast_threshold_mb))
        ),
        "spark.sql.files.maxPartitionBytes": str(_bytes(spark.target_file_mb)),
        "spark.sql.files.openCostInBytes": str(_bytes(4)),
        "spark.sql.parquet.compression.codec": "snappy",
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
        "spark.sql.parquet.int96RebaseModeInWrite": "CORRECTED",
        "spark.sql.session.timeZone": spark.timezone,
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.files.ignoreCorruptFiles": "false",
        "spark.ui.enabled": str(spark.ui_enabled).lower(),
        "spark.ui.showConsoleProgress": "false",
        "spark.executor.memory": spark.executor_memory,
        "spark.executor.cores": str(spark.executor_cores),
        "spark.driver.maxResultSize": "1g",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": str(_bytes(spark.target_file_mb)),
    }
    if spark.local_dir:
        values["spark.local.dir"] = spark.local_dir
    if overrides:
        values.update(overrides)
    return values


def build(
    config: Config,
    app_suffix: str = "",
    overrides: dict[str, str] | None = None,
    master: str | None = None,
) -> SparkSession:
    check_windows_hadoop()
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    os.environ[SUBMIT_ARGS] = f"--driver-memory {config.spark.driver_memory} pyspark-shell"
    name = config.spark.app_name + (f"-{app_suffix}" if app_suffix else "")
    builder = SparkSession.builder.appName(name).master(master or config.spark.master)
    for key, value in settings(config, overrides).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(config.spark.log_level)
    return spark


def stop(spark: SparkSession | None) -> None:
    if spark is not None:
        spark.stop()
