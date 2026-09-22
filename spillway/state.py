from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS source_file (
        feed TEXT NOT NULL,
        month TEXT NOT NULL,
        url TEXT NOT NULL,
        path TEXT,
        status TEXT NOT NULL,
        bytes INTEGER,
        sha256 TEXT,
        header TEXT,
        variant TEXT,
        detail TEXT,
        seconds REAL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (feed, month)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bronze_partition (
        feed TEXT NOT NULL,
        file_month TEXT NOT NULL,
        variant TEXT,
        rows INTEGER,
        corrupt_rows INTEGER,
        out_of_window_rows INTEGER,
        files INTEGER,
        bytes INTEGER,
        seconds REAL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (feed, file_month)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS layer_run (
        layer TEXT NOT NULL,
        rows INTEGER,
        files INTEGER,
        bytes INTEGER,
        seconds REAL,
        detail TEXT,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (layer)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quality_check (
        layer TEXT NOT NULL,
        name TEXT NOT NULL,
        severity TEXT NOT NULL,
        value REAL,
        threshold REAL,
        passed INTEGER NOT NULL,
        detail TEXT,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (layer, name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tuning_result (
        experiment TEXT NOT NULL,
        variant TEXT NOT NULL,
        wall_seconds REAL,
        tasks INTEGER,
        executor_seconds REAL,
        shuffle_read_bytes INTEGER,
        shuffle_write_bytes INTEGER,
        spill_memory_bytes INTEGER,
        spill_disk_bytes INTEGER,
        input_bytes INTEGER,
        max_task_ms INTEGER,
        median_task_ms INTEGER,
        rows INTEGER,
        detail TEXT,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (experiment, variant)
    )
    """,
)


def now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def connect(path: str | Path) -> sqlite3.Connection:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    for statement in SCHEMA:
        conn.execute(statement)
    conn.commit()
    return conn


@contextmanager
def session(path: str | Path):
    conn = connect(path)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def record_file(conn: sqlite3.Connection, **fields) -> None:
    fields.setdefault("updated_at", now())
    columns = ", ".join(fields)
    marks = ", ".join(["?"] * len(fields))
    conn.execute(
        f"INSERT OR REPLACE INTO source_file ({columns}) VALUES ({marks})", tuple(fields.values())
    )
    conn.commit()


def record_bronze(conn: sqlite3.Connection, **fields) -> None:
    fields.setdefault("updated_at", now())
    columns = ", ".join(fields)
    marks = ", ".join(["?"] * len(fields))
    conn.execute(
        f"INSERT OR REPLACE INTO bronze_partition ({columns}) VALUES ({marks})",
        tuple(fields.values()),
    )
    conn.commit()


def record_layer(conn: sqlite3.Connection, **fields) -> None:
    fields.setdefault("updated_at", now())
    columns = ", ".join(fields)
    marks = ", ".join(["?"] * len(fields))
    conn.execute(
        f"INSERT OR REPLACE INTO layer_run ({columns}) VALUES ({marks})", tuple(fields.values())
    )
    conn.commit()


def record_quality(conn: sqlite3.Connection, rows: list[dict]) -> None:
    stamp = now()
    for row in rows:
        row.setdefault("updated_at", stamp)
        columns = ", ".join(row)
        marks = ", ".join(["?"] * len(row))
        conn.execute(
            f"INSERT OR REPLACE INTO quality_check ({columns}) VALUES ({marks})",
            tuple(row.values()),
        )
    conn.commit()


def record_tuning(conn: sqlite3.Connection, **fields) -> None:
    fields.setdefault("updated_at", now())
    columns = ", ".join(fields)
    marks = ", ".join(["?"] * len(fields))
    conn.execute(
        f"INSERT OR REPLACE INTO tuning_result ({columns}) VALUES ({marks})",
        tuple(fields.values()),
    )
    conn.commit()


def file_status(conn: sqlite3.Connection, feed: str, month: str) -> sqlite3.Row | None:
    cursor = conn.execute(
        "SELECT * FROM source_file WHERE feed = ? AND month = ?", (feed, month)
    )
    return cursor.fetchone()


def ready_files(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    cursor = conn.execute("SELECT * FROM source_file WHERE status = 'ready' ORDER BY feed, month")
    return list(cursor.fetchall())


def bronze_status(conn: sqlite3.Connection, feed: str, month: str) -> sqlite3.Row | None:
    cursor = conn.execute(
        "SELECT * FROM bronze_partition WHERE feed = ? AND file_month = ?", (feed, month)
    )
    return cursor.fetchone()


def table_rows(conn: sqlite3.Connection, table: str) -> list[sqlite3.Row]:
    return list(conn.execute(f"SELECT * FROM {table}").fetchall())


def totals(conn: sqlite3.Connection) -> dict[str, int]:
    out = {}
    for name, query in (
        ("source_files_ready", "SELECT COUNT(*) FROM source_file WHERE status = 'ready'"),
        ("source_files_rejected", "SELECT COUNT(*) FROM source_file WHERE status = 'rejected'"),
        ("source_bytes", "SELECT COALESCE(SUM(bytes), 0) FROM source_file WHERE status='ready'"),
        ("bronze_partitions", "SELECT COUNT(*) FROM bronze_partition"),
        ("bronze_rows", "SELECT COALESCE(SUM(rows), 0) FROM bronze_partition"),
        ("bronze_corrupt_rows", "SELECT COALESCE(SUM(corrupt_rows), 0) FROM bronze_partition"),
    ):
        out[name] = int(conn.execute(query).fetchone()[0])
    return out
