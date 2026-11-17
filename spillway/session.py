from __future__ import annotations

import os
import platform
import shutil
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession

from .config import Config

SUBMIT_ARGS = "PYSPARK_SUBMIT_ARGS"
LOCAL_DIR_NAME = "spark-local"
STALE_SECONDS = 86400
_RUN_DIRS: dict[int, Path] = {}


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


def _sweep(root: Path) -> None:
    if not root.exists():
        return
    cutoff = time.time() - STALE_SECONDS
    for item in root.iterdir():
        stamp = item.name.split("-", 1)[0]
        if stamp.isdigit() and int(stamp) < cutoff:
            shutil.rmtree(item, ignore_errors=True)


def run_dir(config: Config) -> Path:
    root = config.data.root / LOCAL_DIR_NAME
    _sweep(root)
    path = root / f"{int(time.time())}-{os.getpid()}"
    path.mkdir(parents=True, exist_ok=True)
    return path


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
    overrides = dict(overrides or {})
    scratch = None
    if not config.spark.local_dir and "spark.local.dir" not in overrides:
        scratch = run_dir(config)
        overrides["spark.local.dir"] = str(scratch)
    builder = SparkSession.builder.appName(name).master(master or config.spark.master)
    for key, value in settings(config, overrides).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    if scratch is not None:
        _RUN_DIRS[id(spark)] = scratch
    spark.sparkContext.setLogLevel(config.spark.log_level)
    return spark


def stop(spark: SparkSession | None) -> None:
    if spark is None:
        return
    spark.stop()
    scratch = _RUN_DIRS.pop(id(spark), None)
    if scratch is not None:
        shutil.rmtree(scratch, ignore_errors=True)
