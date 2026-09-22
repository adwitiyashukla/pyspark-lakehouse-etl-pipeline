from __future__ import annotations

from pathlib import Path

from spillway import sources


def test_month_range_spans_years():
    months = sources.month_range("2019-11", "2020-02")
    assert months == ["2019-11", "2019-12", "2020-01", "2020-02"]


def test_month_range_single_month():
    assert sources.month_range("2021-07", "2021-07") == ["2021-07"]


def test_next_month_rolls_over():
    assert sources.next_month("2019-12") == "2020-01"
    assert sources.next_month("2019-01") == "2019-02"


def test_file_url_uses_the_feed_tag():
    url = sources.file_url("https://host/releases/", "fhvhv", "2021-01")
    assert url == "https://host/releases/fhvhv/fhvhv_tripdata_2021-01.csv.gz"


def test_landing_and_bronze_paths():
    root = Path("/tmp/lake")
    assert sources.landing_path(root, "yellow", "2019-01").name == "yellow_tripdata_2019-01.csv.gz"
    assert sources.bronze_partition(root, "yellow", "2019-01").as_posix().endswith(
        "yellow/file_month=2019-01"
    )


def test_planned_files_covers_every_feed_and_month():
    settings = __import__("spillway.config", fromlist=["load"]).load("config.toml")
    planned = sources.planned_files(settings)
    months = sources.month_range(settings.data.start_month, settings.data.end_month)
    assert len(planned) == len(settings.data.feeds) * len(months)
