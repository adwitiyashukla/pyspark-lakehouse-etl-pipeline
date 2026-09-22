from __future__ import annotations

import pytest

from spillway import schemas

REAL_HEADERS = {
    "yellow_v1": "VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,"
    "trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,"
    "fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,"
    "congestion_surcharge",
    "green_v1": "VendorID,lpep_pickup_datetime,lpep_dropoff_datetime,store_and_fwd_flag,"
    "RatecodeID,PULocationID,DOLocationID,passenger_count,trip_distance,fare_amount,extra,"
    "mta_tax,tip_amount,tolls_amount,ehail_fee,improvement_surcharge,total_amount,payment_type,"
    "trip_type,congestion_surcharge",
    "fhv_v1": "dispatching_base_num,pickup_datetime,dropOff_datetime,PUlocationID,DOlocationID,"
    "SR_Flag,Affiliated_base_number",
    "fhv_v2": "dispatching_base_num,pickup_datetime,dropoff_datetime,PULocationID,DOLocationID,"
    "SR_Flag,Affiliated_base_number",
    "fhvhv_v1": "hvfhs_license_num,dispatching_base_num,pickup_datetime,dropoff_datetime,"
    "PULocationID,DOLocationID,SR_Flag",
}


@pytest.mark.parametrize("name,header", sorted(REAL_HEADERS.items()))
def test_every_published_header_resolves(name, header):
    feed = name.rsplit("_", 1)[0]
    variant = schemas.resolve(feed, schemas.normalize_header(header))
    assert variant is not None
    assert variant.name == name


def test_fhv_renamed_three_columns_between_2019_and_2020():
    old = {column.source: column.name for column in schemas.FHV_V1.columns}
    new = {column.source: column.name for column in schemas.FHV_V2.columns}
    assert set(old) != set(new)
    assert set(old.values()) == set(new.values())
    assert sorted(set(old) - set(new)) == ["DOlocationID", "PUlocationID", "dropOff_datetime"]


def test_an_fhv_header_never_resolves_as_fhvhv():
    assert schemas.resolve("fhvhv", schemas.normalize_header(REAL_HEADERS["fhv_v2"])) is None


def test_normalize_header_strips_bom_quotes_and_carriage_return():
    assert schemas.normalize_header('\ufeff"a", b ,c\r\n') == ("a", "b", "c")


def test_read_schema_carries_the_corrupt_column():
    fields = [field.name for field in schemas.YELLOW_V1.read_schema().fields]
    assert fields[-1] == schemas.CORRUPT_COLUMN
    assert len(fields) == len(schemas.YELLOW_V1.columns) + 1


def test_canonical_columns_merge_both_fhv_layouts():
    columns = schemas.canonical_columns("fhv")
    assert "pu_location_id" in columns
    assert "dropoff_datetime" in columns
    assert len(columns) == len(schemas.FHV_V1.columns)
