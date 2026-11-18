from __future__ import annotations

from lakeetl import state


def test_tables_are_created(tmp_path):
    with state.session(tmp_path / "state.db") as conn:
        names = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
    expected = {"source_file", "bronze_partition", "layer_run", "quality_check", "tuning_result"}
    assert expected <= names


def test_record_file_is_idempotent(tmp_path):
    path = tmp_path / "state.db"
    with state.session(path) as conn:
        state.record_file(conn, feed="yellow", month="2019-01", url="u", status="ready", bytes=10)
        state.record_file(conn, feed="yellow", month="2019-01", url="u", status="ready", bytes=20)
        rows = state.table_rows(conn, "source_file")
        assert len(rows) == 1
        assert rows[0]["bytes"] == 20
        assert state.totals(conn)["source_files_ready"] == 1


def test_ready_files_skips_rejected(tmp_path):
    with state.session(tmp_path / "state.db") as conn:
        state.record_file(conn, feed="fhvhv", month="2021-06", url="u", status="rejected")
        state.record_file(conn, feed="fhvhv", month="2021-01", url="u", status="ready")
        ready = state.ready_files(conn)
    assert [row["month"] for row in ready] == ["2021-01"]


def test_bronze_and_quality_round_trip(tmp_path):
    with state.session(tmp_path / "state.db") as conn:
        state.record_bronze(conn, feed="green", file_month="2019-01", rows=5, corrupt_rows=1)
        assert state.bronze_status(conn, "green", "2019-01")["rows"] == 5
        state.record_quality(
            conn,
            [
                {
                    "layer": "bronze",
                    "name": "corrupt_rate_green",
                    "severity": "error",
                    "value": 0.2,
                    "threshold": 0.5,
                    "passed": 1,
                    "detail": "",
                }
            ],
        )
        assert len(state.table_rows(conn, "quality_check")) == 1
        state.record_tuning(conn, experiment="a", variant="b", wall_seconds=1.0, tasks=2)
        assert state.table_rows(conn, "tuning_result")[0]["variant"] == "b"
