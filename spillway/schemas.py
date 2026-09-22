from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import types as T

CORRUPT_COLUMN = "_corrupt_record"
TIMESTAMP_COLUMNS = ("pickup_datetime", "dropoff_datetime")


@dataclass(frozen=True)
class Column:
    source: str
    name: str
    read_type: T.DataType
    cast_type: T.DataType | None = None


@dataclass(frozen=True)
class Variant:
    feed: str
    name: str
    columns: tuple[Column, ...]

    @property
    def header(self) -> tuple[str, ...]:
        return tuple(column.source for column in self.columns)

    def read_schema(self) -> T.StructType:
        fields = [T.StructField(column.name, column.read_type, True) for column in self.columns]
        fields.append(T.StructField(CORRUPT_COLUMN, T.StringType(), True))
        return T.StructType(fields)


def _text(source: str, name: str) -> Column:
    return Column(source, name, T.StringType())


def _stamp(source: str, name: str) -> Column:
    return Column(source, name, T.StringType(), T.TimestampType())


def _whole(source: str, name: str) -> Column:
    return Column(source, name, T.DoubleType(), T.IntegerType())


def _money(source: str, name: str) -> Column:
    return Column(source, name, T.DoubleType())


YELLOW_V1 = Variant(
    "yellow",
    "yellow_v1",
    (
        _whole("VendorID", "vendor_id"),
        _stamp("tpep_pickup_datetime", "pickup_datetime"),
        _stamp("tpep_dropoff_datetime", "dropoff_datetime"),
        _money("passenger_count", "passenger_count"),
        _money("trip_distance", "trip_distance"),
        _money("RatecodeID", "ratecode_id"),
        _text("store_and_fwd_flag", "store_and_fwd_flag"),
        _whole("PULocationID", "pu_location_id"),
        _whole("DOLocationID", "do_location_id"),
        _whole("payment_type", "payment_type"),
        _money("fare_amount", "fare_amount"),
        _money("extra", "extra"),
        _money("mta_tax", "mta_tax"),
        _money("tip_amount", "tip_amount"),
        _money("tolls_amount", "tolls_amount"),
        _money("improvement_surcharge", "improvement_surcharge"),
        _money("total_amount", "total_amount"),
        _money("congestion_surcharge", "congestion_surcharge"),
    ),
)

GREEN_V1 = Variant(
    "green",
    "green_v1",
    (
        _whole("VendorID", "vendor_id"),
        _stamp("lpep_pickup_datetime", "pickup_datetime"),
        _stamp("lpep_dropoff_datetime", "dropoff_datetime"),
        _text("store_and_fwd_flag", "store_and_fwd_flag"),
        _money("RatecodeID", "ratecode_id"),
        _whole("PULocationID", "pu_location_id"),
        _whole("DOLocationID", "do_location_id"),
        _money("passenger_count", "passenger_count"),
        _money("trip_distance", "trip_distance"),
        _money("fare_amount", "fare_amount"),
        _money("extra", "extra"),
        _money("mta_tax", "mta_tax"),
        _money("tip_amount", "tip_amount"),
        _money("tolls_amount", "tolls_amount"),
        _money("ehail_fee", "ehail_fee"),
        _money("improvement_surcharge", "improvement_surcharge"),
        _money("total_amount", "total_amount"),
        _whole("payment_type", "payment_type"),
        _whole("trip_type", "trip_type"),
        _money("congestion_surcharge", "congestion_surcharge"),
    ),
)

FHV_V1 = Variant(
    "fhv",
    "fhv_v1",
    (
        _text("dispatching_base_num", "dispatching_base_num"),
        _stamp("pickup_datetime", "pickup_datetime"),
        _stamp("dropOff_datetime", "dropoff_datetime"),
        _whole("PUlocationID", "pu_location_id"),
        _whole("DOlocationID", "do_location_id"),
        _text("SR_Flag", "sr_flag"),
        _text("Affiliated_base_number", "affiliated_base_num"),
    ),
)

FHV_V2 = Variant(
    "fhv",
    "fhv_v2",
    (
        _text("dispatching_base_num", "dispatching_base_num"),
        _stamp("pickup_datetime", "pickup_datetime"),
        _stamp("dropoff_datetime", "dropoff_datetime"),
        _whole("PULocationID", "pu_location_id"),
        _whole("DOLocationID", "do_location_id"),
        _text("SR_Flag", "sr_flag"),
        _text("Affiliated_base_number", "affiliated_base_num"),
    ),
)

FHVHV_V1 = Variant(
    "fhvhv",
    "fhvhv_v1",
    (
        _text("hvfhs_license_num", "hvfhs_license_num"),
        _text("dispatching_base_num", "dispatching_base_num"),
        _stamp("pickup_datetime", "pickup_datetime"),
        _stamp("dropoff_datetime", "dropoff_datetime"),
        _whole("PULocationID", "pu_location_id"),
        _whole("DOLocationID", "do_location_id"),
        _text("SR_Flag", "sr_flag"),
    ),
)

VARIANTS: dict[str, tuple[Variant, ...]] = {
    "yellow": (YELLOW_V1,),
    "green": (GREEN_V1,),
    "fhv": (FHV_V1, FHV_V2),
    "fhvhv": (FHVHV_V1,),
}

BRONZE_EXTRA = ("feed", "file_month", "schema_variant", "source_file")


def normalize_header(line: str) -> tuple[str, ...]:
    cleaned = line.replace("\ufeff", "").strip().strip("\r")
    return tuple(part.strip().strip('"') for part in cleaned.split(","))


def resolve(feed: str, header: tuple[str, ...]) -> Variant | None:
    for variant in VARIANTS.get(feed, ()):
        if variant.header == header:
            return variant
    return None


def canonical_columns(feed: str) -> tuple[str, ...]:
    seen: list[str] = []
    for variant in VARIANTS[feed]:
        for column in variant.columns:
            if column.name not in seen:
                seen.append(column.name)
    return tuple(seen)


def known_headers(feed: str) -> tuple[tuple[str, ...], ...]:
    return tuple(variant.header for variant in VARIANTS[feed])
