from __future__ import annotations

import argparse
import logging
import sys

from . import (
    bronze,
    cluster,
    gold,
    iceberg,
    ingest,
    quality,
    report,
    session,
    silver,
    streaming,
    tuning,
)
from . import config as config_module

LOG = logging.getLogger("spillway")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="spillway")
    parser.add_argument("--config", default="config.toml")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--master", default="")
    subparsers = parser.add_subparsers(dest="command", required=True)

    fetch = subparsers.add_parser("ingest")
    fetch.add_argument("--force", action="store_true")
    fetch.add_argument("--feed", action="append", default=[])

    raw = subparsers.add_parser("bronze")
    raw.add_argument("--force", action="store_true")
    raw.add_argument("--feed", action="append", default=[])

    clean = subparsers.add_parser("silver")
    clean.add_argument("--month", action="append", default=[])

    subparsers.add_parser("gold")

    gate = subparsers.add_parser("quality")
    gate.add_argument("--no-gate", action="store_true")

    subparsers.add_parser("iceberg")

    travel = subparsers.add_parser("time-travel")
    travel.add_argument("--table", default="zone_month")

    produce = subparsers.add_parser("stream-produce")
    produce.add_argument("--month", default="")

    stream = subparsers.add_parser("stream-run")
    stream.add_argument("--reset", action="store_true")

    tune = subparsers.add_parser("tune")
    tune.add_argument("--only", action="append", default=[])

    start = subparsers.add_parser("cluster-start")
    start.add_argument("--workers", type=int, default=2)
    start.add_argument("--worker-cores", type=int, default=1)
    start.add_argument("--worker-memory", default="")

    subparsers.add_parser("cluster-stop")

    full = subparsers.add_parser("pipeline")
    full.add_argument("--skip-ingest", action="store_true")
    full.add_argument("--no-gate", action="store_true")

    subparsers.add_parser("report")
    subparsers.add_parser("status")
    return parser


def _spark(settings, args, suffix: str):
    master = args.master or None
    if not master and cluster.master_url(settings):
        master = cluster.master_url(settings)
    return session.build(settings, suffix, master=master)


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    settings = config_module.load(args.config)
    command = args.command
    if command == "ingest":
        ingest.run(settings, args.force, tuple(args.feed) or None)
        return 0
    if command == "report":
        print(report.build(settings))
        report.write(settings)
        return 0
    if command == "status":
        print(report.build(settings))
        return 0
    if command == "cluster-start":
        print(cluster.start(settings, args.workers, args.worker_cores, args.worker_memory))
        return 0
    if command == "cluster-stop":
        print(cluster.stop(settings))
        return 0
    if command == "iceberg":
        iceberg.build(settings)
        return 0
    if command == "time-travel":
        print(iceberg.time_travel(settings, args.table))
        return 0
    spark = _spark(settings, args, command)
    try:
        if command == "bronze":
            bronze.build(settings, spark, tuple(args.feed) or None, args.force)
        elif command == "silver":
            silver.build(settings, spark, tuple(args.month) or None)
        elif command == "gold":
            gold.build(settings, spark)
        elif command == "quality":
            quality.run(settings, spark, not args.no_gate)
        elif command == "stream-produce":
            streaming.produce(settings, spark, args.month or None)
        elif command == "stream-run":
            streaming.run(settings, spark, args.reset)
        elif command == "tune":
            tuning.run(settings, spark, tuple(args.only) or None)
        elif command == "pipeline":
            if not args.skip_ingest:
                ingest.run(settings)
            bronze.build(settings, spark)
            silver.build(settings, spark)
            gold.build(settings, spark)
            quality.run(settings, spark, not args.no_gate)
            iceberg.build(settings)
            print(report.build(settings))
            report.write(settings)
    finally:
        session.stop(spark)
    return 0


if __name__ == "__main__":
    sys.exit(main())
