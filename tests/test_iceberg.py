from __future__ import annotations

import pytest

from spillway import iceberg


@pytest.fixture(scope="module")
def committed(lake):
    return iceberg.build(lake)


def test_every_configured_mart_becomes_a_table(lake, committed):
    assert set(committed) == set(lake.iceberg.tables)
    store = iceberg.catalog(lake)
    for name in lake.iceberg.tables:
        assert store.table_exists(f"{lake.iceberg.namespace}.{name}")


def test_files_are_committed_in_two_snapshots_when_there_are_two(lake, committed):
    for name, summary in committed.items():
        travel = iceberg.time_travel(lake, name)
        assert travel["snapshots"] == min(summary["files"], 2)
        if travel["snapshots"] == 2:
            assert travel["first_records"] < travel["latest_records"]
            assert travel["first_snapshot_id"] != travel["latest_snapshot_id"]


def test_schema_evolution_adds_the_column_without_rewriting_data(lake, committed):
    travel = iceberg.time_travel(lake, "od_month")
    assert iceberg.EVOLUTION_COLUMN in travel["columns"]
    assert committed["od_month"]["columns"] == len(travel["columns"])
    store = iceberg.catalog(lake)
    table = store.load_table(f"{lake.iceberg.namespace}.od_month")
    assert table.schema().find_field(iceberg.EVOLUTION_COLUMN).field_type == iceberg.StringType()


def test_snapshot_totals_match_the_mart(lake, spark, committed):
    from spillway import gold

    rows = gold.read(spark, lake, "zone_month").count()
    assert committed["zone_month"]["latest_snapshot_records"] == rows


def test_the_warehouse_uri_keeps_the_drive_letter(tmp_path, monkeypatch):
    monkeypatch.setattr(iceberg.platform, "system", lambda: "Windows")
    text = iceberg.uri(tmp_path)
    assert text.startswith("file://") and not text.startswith("file:///")
