from __future__ import annotations

import pytest

from lakeetl import state, tuning


def test_partition_pruning_variants_agree(lake, spark):
    summary = tuning.run(lake, spark, only=("partition_pruning",))
    assert summary["measurements"] == 2
    with state.session(lake.data.state_db) as conn:
        rows = list(state.table_rows(conn, "tuning_result"))
    variants = {row["variant"]: row for row in rows}
    assert {"partition_filter", "derived_filter"} <= set(variants)
    assert variants["partition_filter"]["rows"] == variants["derived_filter"]["rows"]


def test_unknown_experiment_is_rejected(lake, spark):
    with pytest.raises(ValueError):
        tuning.run(lake, spark, only=("no_such_experiment",))


def test_conf_helper_restores_the_previous_value(spark):
    key = "spark.sql.shuffle.partitions"
    before = spark.conf.get(key)
    with tuning.conf(spark, {key: "77"}):
        assert spark.conf.get(key) == "77"
    assert spark.conf.get(key) == before


def test_report_file_is_written(lake, spark):
    summary = tuning.run(lake, spark, only=("partition_pruning",))
    assert summary["report"].endswith("tuning.json")
