from __future__ import annotations

import contextlib
import logging
import platform
import time
from pathlib import Path

import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.types import StringType

from . import state
from .config import Config

LOG = logging.getLogger(__name__)
EVOLUTION_COLUMN = "loaded_by"


def uri(path: Path) -> str:
    text = path.resolve().as_uri()
    if platform.system() == "Windows" and text.startswith("file:///"):
        return "file://" + text[len("file:///") :]
    return text


def catalog(config: Config) -> SqlCatalog:
    warehouse = config.data.warehouse
    warehouse.mkdir(parents=True, exist_ok=True)
    return SqlCatalog(
        config.iceberg.catalog_name,
        **{
            "uri": f"sqlite:///{(warehouse / 'catalog.db').as_posix()}",
            "warehouse": uri(warehouse),
        },
    )


def _mart_files(config: Config, name: str) -> list[Path]:
    return sorted(item for item in (config.data.gold / name).rglob("*.parquet") if item.is_file())


def build(config: Config) -> dict:
    started = time.perf_counter()
    store = catalog(config)
    with contextlib.suppress(Exception):
        store.create_namespace(config.iceberg.namespace)
    summary = {}
    for name in config.iceberg.tables:
        files = _mart_files(config, name)
        if not files:
            LOG.warning("iceberg: %s has no parquet files, skipped", name)
            continue
        identifier = f"{config.iceberg.namespace}.{name}"
        if store.table_exists(identifier):
            store.drop_table(identifier)
        schema = pq.read_schema(files[0])
        table = store.create_table(identifier, schema=schema)
        head = files[: max(len(files) // 2, 1)]
        tail = files[len(head) :]
        table.add_files([uri(item) for item in head])
        first_snapshot = table.current_snapshot()
        if tail:
            table.add_files([uri(item) for item in tail])
        table.refresh()
        latest = table.current_snapshot()
        with table.update_schema() as update:
            update.add_column(EVOLUTION_COLUMN, StringType(), "pipeline that wrote the rows")
        table.refresh()
        summary[name] = {
            "snapshots": len(list(table.history())),
            "first_snapshot_records": int(
                first_snapshot.summary.get("total-records", 0) if first_snapshot else 0
            ),
            "latest_snapshot_records": int(
                latest.summary.get("total-records", 0) if latest else 0
            ),
            "files": len(files),
            "columns": len(table.schema().fields),
        }
        LOG.info("iceberg: %s %s", name, summary[name])
    elapsed = time.perf_counter() - started
    with state.session(config.data.state_db) as conn:
        state.record_layer(
            conn,
            layer="iceberg",
            rows=sum(item["latest_snapshot_records"] for item in summary.values()),
            files=sum(item["files"] for item in summary.values()),
            bytes=sum(
                item.stat().st_size for item in config.data.warehouse.rglob("*") if item.is_file()
            ),
            seconds=round(elapsed, 3),
            detail=f"{len(summary)} tables",
        )
    LOG.info("iceberg done in %.1f s: %d tables", elapsed, len(summary))
    return summary


def time_travel(config: Config, name: str) -> dict:
    store = catalog(config)
    table = store.load_table(f"{config.iceberg.namespace}.{name}")
    history = list(table.history())
    first = table.snapshot_by_id(history[0].snapshot_id) if history else None
    latest = table.current_snapshot()
    if first is None or latest is None:
        return {"snapshots": 0, "columns": [field.name for field in table.schema().fields]}
    return {
        "snapshots": len(history),
        "first_snapshot_id": first.snapshot_id,
        "first_records": int(first.summary.get("total-records", 0)),
        "latest_snapshot_id": latest.snapshot_id,
        "latest_records": int(latest.summary.get("total-records", 0)),
        "columns": [field.name for field in table.schema().fields],
    }
