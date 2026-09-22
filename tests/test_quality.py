from __future__ import annotations

import dataclasses

import pytest

from spillway import quality, state


def test_all_checks_pass_on_the_test_lake(lake, spark):
    result = quality.run(lake, spark, gate=True)
    names = [check["name"] for check in result["checks"]]
    assert "row_reconciliation" in names
    assert any(name.startswith("corrupt_rate_") for name in names)
    assert result["blocking"] == 0


def test_results_are_stored_for_the_report(lake, spark):
    quality.run(lake, spark, gate=False)
    with state.session(lake.data.state_db) as conn:
        rows = state.table_rows(conn, "quality_check")
    assert len(rows) > 10
    assert {row["layer"] for row in rows} == {"bronze", "silver", "gold"}


def test_the_gate_blocks_on_a_strict_threshold(lake, spark):
    strict = dataclasses.replace(
        lake, quality=dataclasses.replace(lake.quality, max_corrupt_rate=0.0)
    )
    with pytest.raises(quality.QualityError) as error:
        quality.run(strict, spark, gate=True)
    assert "corrupt_rate_yellow" in str(error.value)


def test_warnings_do_not_block(lake, spark):
    loose = dataclasses.replace(
        lake, quality=dataclasses.replace(lake.quality, max_unknown_zone_rate=0.0)
    )
    result = quality.run(loose, spark, gate=True)
    assert result["failed"] > 0
    assert result["blocking"] == 0


def test_reconciliation_accounts_for_every_bronze_row(lake, spark):
    checks = {check["name"]: check for check in quality.run(lake, spark, gate=False)["checks"]}
    recon = checks["row_reconciliation"]
    assert recon["passed"]
    assert "duplicates" in recon["detail"]
