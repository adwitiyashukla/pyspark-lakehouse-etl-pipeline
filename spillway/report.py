from __future__ import annotations

from pathlib import Path

from . import state
from .config import Config

WIDTH = 78


def _line(char: str = "-") -> str:
    return char * WIDTH


def _mb(value) -> str:
    return f"{(value or 0) / 1e6:,.1f} MB"


def build(config: Config) -> str:
    lines: list[str] = []
    with state.session(config.data.state_db) as conn:
        totals = state.totals(conn)
        layers = state.table_rows(conn, "layer_run")
        files = state.table_rows(conn, "source_file")
        partitions = state.table_rows(conn, "bronze_partition")
        checks = state.table_rows(conn, "quality_check")
        tuning = state.table_rows(conn, "tuning_result")
    lines.append("spillway run summary")
    lines.append(_line("="))
    lines.append(
        f"source files ready {totals['source_files_ready']}, "
        f"rejected {totals['source_files_rejected']}, "
        f"compressed {_mb(totals['source_bytes'])}"
    )
    by_feed: dict[str, list[int]] = {}
    for row in partitions:
        bucket = by_feed.setdefault(row["feed"], [0, 0, 0, 0])
        bucket[0] += 1
        bucket[1] += row["rows"] or 0
        bucket[2] += row["corrupt_rows"] or 0
        bucket[3] += row["out_of_window_rows"] or 0
    lines.append("")
    lines.append("bronze by feed")
    lines.append(_line())
    lines.append(f"{'feed':10s}{'months':>8s}{'rows':>16s}{'corrupt':>12s}{'off month':>12s}")
    for feed, bucket in sorted(by_feed.items()):
        lines.append(
            f"{feed:10s}{bucket[0]:>8d}{bucket[1]:>16,d}{bucket[2]:>12,d}{bucket[3]:>12,d}"
        )
    rejected = [row for row in files if row["status"] == "rejected"]
    if rejected:
        lines.append("")
        lines.append("rejected source files")
        lines.append(_line())
        for row in rejected:
            lines.append(f"{row['feed']} {row['month']}: {row['detail']}")
            lines.append(f"  header {row['header']}")
    lines.append("")
    lines.append("layers")
    lines.append(_line())
    lines.append(f"{'layer':12s}{'rows':>16s}{'files':>8s}{'size':>14s}{'seconds':>10s}")
    for row in sorted(layers, key=lambda item: item["layer"]):
        lines.append(
            f"{row['layer']:12s}{row['rows'] or 0:>16,d}{row['files'] or 0:>8d}"
            f"{_mb(row['bytes']):>14s}{row['seconds'] or 0:>10.1f}"
        )
        if row["detail"]:
            lines.append(f"  {row['detail']}")
    if checks:
        passed = sum(1 for row in checks if row["passed"])
        lines.append("")
        lines.append(f"quality checks {passed} passed, {len(checks) - passed} failed")
        lines.append(_line())
        for row in sorted(checks, key=lambda item: (item["layer"], item["name"])):
            mark = "pass" if row["passed"] else row["severity"]
            lines.append(
                f"{mark:8s}{row['layer']:8s}{row['name']:38s}"
                f"{row['value']:>12.6f} limit {row['threshold']:.6f}"
            )
    if tuning:
        lines.append("")
        lines.append("tuning lab")
        lines.append(_line())
        lines.append(
            f"{'experiment':22s}{'variant':20s}{'seconds':>9s}{'tasks':>7s}"
            f"{'shuffle r':>12s}{'max task':>10s}"
        )
        for row in sorted(tuning, key=lambda item: (item["experiment"], item["variant"])):
            lines.append(
                f"{row['experiment']:22s}{row['variant']:20s}{row['wall_seconds']:>9.1f}"
                f"{row['tasks']:>7d}{_mb(row['shuffle_read_bytes']):>12s}"
                f"{row['max_task_ms']:>9d}ms"
            )
    return "\n".join(lines)


def write(config: Config) -> Path:
    text = build(config)
    config.data.reports.mkdir(parents=True, exist_ok=True)
    target = config.data.reports / "summary.txt"
    target.write_text(text + "\n", encoding="utf-8")
    return target
