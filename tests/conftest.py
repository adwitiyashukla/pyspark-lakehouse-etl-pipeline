from __future__ import annotations

import gzip
from pathlib import Path

import pytest

from lakeetl import bronze, gold, schemas, silver, sources, state
from lakeetl import config as config_module

CONFIG_TEMPLATE = """
[data]
root = "{root}"
feeds = ["yellow", "green", "fhv", "fhvhv"]
start_month = "2019-01"
end_month = "2019-02"
release_base = "https://example.invalid/releases"
zone_lookup_url = "https://example.invalid/zones.csv"

[spark]
master = "local[2]"
app_name = "lakeetl-test"
driver_memory = "1g"
executor_memory = "1g"
executor_cores = 1
shuffle_partitions = 2
adaptive = true
adaptive_skew_join = true
broadcast_threshold_mb = 8
target_file_mb = 32
timezone = "UTC"
local_dir = ""
log_level = "ERROR"
ui_enabled = false

[ingest]
workers = 2
timeout_seconds = 30
retries = 1
chunk_kb = 64
verify_header = true

[bronze]
timestamp_format = "yyyy-MM-dd HH:mm:ss"
rows_per_file = 100000
keep_corrupt_rows = true

[silver]
min_trip_seconds = 30
max_trip_seconds = 21600
max_trip_miles = 200.0
max_speed_mph = 90.0
max_fare = 2000.0
dedup = true

[gold]
top_pairs = 10
min_zone_trips = 1

[quality]
max_corrupt_rate = 0.2
max_null_pickup_rate = 0.05
max_unknown_zone_rate = 0.8
max_out_of_window_rate = 0.5
min_rows_per_month = 1
fail_on = "error"

[tuning]
sample_feed = "yellow"
sample_months = ["2019-01"]
skew_feed = "fhv"
skew_months = ["2019-01"]
experiments = ["partition_pruning"]
salt_buckets = 4
repeat = 1

[streaming]
files_per_batch = 2
trigger_seconds = 0
watermark_minutes = 30
window_minutes = 15
producer_files = 4
producer_rows = 1000
late_rows = 100
late_offset_hours = 8

[iceberg]
catalog_name = "lakeetl_test"
namespace = "gold"
tables = ["od_month", "zone_month", "feed_share"]
"""

ZONES = "LocationID,Borough,Zone\n1,EWR,Newark Airport\n132,Queens,JFK Airport\n"
ZONES += "138,Queens,LaGuardia Airport\n236,Manhattan,Upper East Side North\n"
ZONES += "237,Manhattan,Upper East Side South\n264,Unknown,Unknown\n265,Unknown,Unknown\n"


def _yellow_rows(month: str) -> list[str]:
    day = f"{month}-05"
    return [
        f"1,{day} 08:00:00,{day} 08:20:00,1,3.10,1,N,236,237,1,14.5,0.5,0.5,3.0,0,0.3,18.8,2.5",
        f"2,{day} 09:00:00,{day} 09:45:00,2,12.40,1,N,132,236,1,45.0,0.0,0.5,9.0,6.12,"
        f"0.3,60.92,2.5",
        f"2,{day} 10:00:00,{day} 10:00:10,1,0.10,1,N,237,237,2,2.5,0.0,0.5,0,0,0.3,3.3,0",
        f"1,{day} 11:00:00,{day} 10:30:00,1,2.00,1,N,236,237,1,9.0,0.0,0.5,0,0,0.3,9.8,0",
        f"1,{day} 12:00:00,{day} 12:02:00,1,180.00,1,N,236,237,1,500.0,0,0.5,0,0,0.3,500.8,0",
        f"2,{day} 13:00:00,{day} 13:50:00,3,17.20,2,N,237,132,1,52.0,0.0,0.5,10.0,6.12,"
        f"0.3,68.92,2.5",
        "1,2018-11-30 23:00:00,2018-11-30 23:40:00,1,5.00,1,N,236,237,1,20.0,0,0.5,0,0,0.3,20.8,0",
        "broken,row,with,too,few",
    ]


def _green_rows(month: str) -> list[str]:
    day = f"{month}-06"
    trip = (
        f"2,{day} 07:00:00,{day} 07:30:00,N,1,236,237,1,4.20,16.0,0.5,0.5,2.0,0,,0.3,19.3,1,1,0"
    )
    return [
        trip,
        trip,
        f"2,{day} 08:00:00,{day} 08:25:00,N,1,132,236,1,11.10,38.0,0.5,0.5,5.0,0,,"
        f"0.3,44.3,1,1,2.75",
    ]


def _fhv_v1_rows(month: str) -> list[str]:
    day = f"{month}-07"
    return [
        f"B00001,{day} 06:00:00,{day} 06:35:00,,,,B00001",
        f"B00002,{day} 07:15:00,{day} 07:50:00,236,237,,B00002",
    ]


def _fhv_v2_rows(month: str) -> list[str]:
    day = f"{month}-08"
    return [
        f"B00013,{day} 06:00:00,{day} 06:40:00,132,236,N,B00013",
        f"B00014,{day} 09:00:00,{day} 09:31:00,,,,B00014",
    ]


def _fhvhv_rows(month: str) -> list[str]:
    day = f"{month}-09"
    return [
        f"HV0003,B02682,{day} 18:00:00,{day} 18:32:00,237,236,",
        f"HV0005,B02510,{day} 19:00:00,{day} 19:40:00,132,237,1",
    ]


def _write_gz(path: Path, header: tuple[str, ...], rows: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = "\r\n".join([",".join(header), *rows]) + "\r\n"
    with gzip.open(path, "wt", encoding="utf-8", newline="") as handle:
        handle.write(body)


def _write_config(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    target = root / "config.toml"
    target.write_text(
        CONFIG_TEMPLATE.format(root=(root / "data").as_posix()), encoding="utf-8"
    )
    return target


@pytest.fixture(scope="session")
def spark(settings):
    from lakeetl import session as session_module

    built = session_module.build(settings, "tests")
    yield built
    built.stop()


@pytest.fixture(scope="session")
def settings(tmp_path_factory):
    root = tmp_path_factory.mktemp("lakeetl")
    return config_module.load(_write_config(root))


@pytest.fixture(scope="session")
def landed(settings):
    months = ("2019-01", "2019-02")
    plan = [
        ("yellow", schemas.YELLOW_V1, _yellow_rows),
        ("green", schemas.GREEN_V1, _green_rows),
        ("fhvhv", schemas.FHVHV_V1, _fhvhv_rows),
    ]
    with state.session(settings.data.state_db) as conn:
        for feed, variant, builder in plan:
            for month in months:
                path = sources.landing_path(settings.data.landing, feed, month)
                _write_gz(path, variant.header, builder(month))
                state.record_file(
                    conn,
                    feed=feed,
                    month=month,
                    url="https://example.invalid",
                    path=str(path),
                    status="ready",
                    bytes=path.stat().st_size,
                    sha256="0" * 64,
                    header=",".join(variant.header),
                    variant=variant.name,
                )
        for month, variant, builder in (
            ("2019-01", schemas.FHV_V1, _fhv_v1_rows),
            ("2019-02", schemas.FHV_V2, _fhv_v2_rows),
        ):
            path = sources.landing_path(settings.data.landing, "fhv", month)
            _write_gz(path, variant.header, builder(month))
            state.record_file(
                conn,
                feed="fhv",
                month=month,
                url="https://example.invalid",
                path=str(path),
                status="ready",
                bytes=path.stat().st_size,
                sha256="0" * 64,
                header=",".join(variant.header),
                variant=variant.name,
            )
    settings.data.reference.mkdir(parents=True, exist_ok=True)
    (settings.data.reference / "taxi_zone_lookup.csv").write_text(ZONES, encoding="utf-8")
    return settings


@pytest.fixture(scope="session")
def lake(spark, landed):
    bronze.build(landed, spark)
    silver.build(landed, spark)
    gold.build(landed, spark)
    return landed
