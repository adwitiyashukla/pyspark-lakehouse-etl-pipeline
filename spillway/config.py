from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path

VALID_FEEDS = ("yellow", "green", "fhv", "fhvhv")
VALID_SEVERITY = ("error", "warning")
MEMORY_SUFFIXES = ("k", "m", "g", "t")


class ConfigError(ValueError):
    pass


def _require(table: dict, key: str, section: str):
    if key not in table:
        raise ConfigError(f"[{section}] is missing the key {key}")
    return table[key]


def _as_int(value, name: str, minimum: int | None = None, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{name} must be an integer, got {value!r}")
    if minimum is not None and value < minimum:
        raise ConfigError(f"{name} must be at least {minimum}, got {value}")
    if maximum is not None and value > maximum:
        raise ConfigError(f"{name} must be at most {maximum}, got {value}")
    return value


def _as_float(value, name: str, minimum: float | None = None) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"{name} must be a number, got {value!r}")
    value = float(value)
    if minimum is not None and value < minimum:
        raise ConfigError(f"{name} must be at least {minimum}, got {value}")
    return value


def _as_bool(value, name: str) -> bool:
    if not isinstance(value, bool):
        raise ConfigError(f"{name} must be true or false, got {value!r}")
    return value


def _as_str(value, name: str, allowed: tuple[str, ...] | None = None) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{name} must be a non empty string, got {value!r}")
    if allowed is not None and value not in allowed:
        raise ConfigError(f"{name} must be one of {', '.join(allowed)}, got {value}")
    return value


def _as_rate(value, name: str) -> float:
    rate = _as_float(value, name, minimum=0.0)
    if rate > 1.0:
        raise ConfigError(f"{name} must be a rate between 0 and 1, got {rate}")
    return rate


def _as_memory(value, name: str) -> str:
    text = _as_str(value, name)
    if not text[:-1].isdigit() or text[-1].lower() not in MEMORY_SUFFIXES:
        raise ConfigError(f"{name} must look like 4g or 512m, got {text}")
    return text


def _as_month(value, name: str) -> str:
    text = _as_str(value, name)
    parts = text.split("-")
    if len(parts) != 2 or len(parts[0]) != 4 or len(parts[1]) != 2:
        raise ConfigError(f"{name} must look like 2019-01, got {text}")
    if not parts[0].isdigit() or not parts[1].isdigit():
        raise ConfigError(f"{name} must look like 2019-01, got {text}")
    if not 1 <= int(parts[1]) <= 12:
        raise ConfigError(f"{name} has month {parts[1]} outside 01 to 12")
    return text


def _as_url(value, name: str) -> str:
    text = _as_str(value, name)
    if not text.startswith("https://"):
        raise ConfigError(f"{name} must start with https://, got {text}")
    return text


@dataclass(frozen=True)
class DataConfig:
    root: Path
    feeds: tuple[str, ...]
    start_month: str
    end_month: str
    release_base: str
    zone_lookup_url: str

    @property
    def landing(self) -> Path:
        return self.root / "landing"

    @property
    def bronze(self) -> Path:
        return self.root / "bronze"

    @property
    def silver(self) -> Path:
        return self.root / "silver"

    @property
    def gold(self) -> Path:
        return self.root / "gold"

    @property
    def quarantine(self) -> Path:
        return self.root / "quarantine"

    @property
    def reference(self) -> Path:
        return self.root / "reference"

    @property
    def reports(self) -> Path:
        return self.root / "reports"

    @property
    def state_db(self) -> Path:
        return self.root / "state.db"

    @property
    def stream(self) -> Path:
        return self.root / "stream"

    @property
    def warehouse(self) -> Path:
        return self.root / "warehouse"


@dataclass(frozen=True)
class SparkConfig:
    master: str
    app_name: str
    driver_memory: str
    executor_memory: str
    executor_cores: int
    shuffle_partitions: int
    adaptive: bool
    adaptive_skew_join: bool
    broadcast_threshold_mb: int
    target_file_mb: int
    timezone: str
    local_dir: str
    log_level: str
    ui_enabled: bool


@dataclass(frozen=True)
class IngestConfig:
    workers: int
    timeout_seconds: int
    retries: int
    chunk_kb: int
    verify_header: bool


@dataclass(frozen=True)
class BronzeConfig:
    timestamp_format: str
    rows_per_file: int
    keep_corrupt_rows: bool


@dataclass(frozen=True)
class SilverConfig:
    min_trip_seconds: int
    max_trip_seconds: int
    max_trip_miles: float
    max_speed_mph: float
    max_fare: float
    dedup: bool


@dataclass(frozen=True)
class GoldConfig:
    top_pairs: int
    min_zone_trips: int


@dataclass(frozen=True)
class QualityConfig:
    max_corrupt_rate: float
    max_null_pickup_rate: float
    max_unknown_zone_rate: float
    max_out_of_window_rate: float
    min_rows_per_month: int
    fail_on: str


@dataclass(frozen=True)
class TuningConfig:
    sample_feed: str
    sample_months: tuple[str, ...]
    skew_feed: str
    skew_months: tuple[str, ...]
    experiments: tuple[str, ...]
    salt_buckets: int
    repeat: int


@dataclass(frozen=True)
class StreamingConfig:
    files_per_batch: int
    trigger_seconds: int
    watermark_minutes: int
    window_minutes: int
    producer_files: int
    producer_rows: int
    late_rows: int
    late_offset_hours: int


@dataclass(frozen=True)
class IcebergConfig:
    catalog_name: str
    namespace: str
    tables: tuple[str, ...]


@dataclass(frozen=True)
class Config:
    path: Path
    data: DataConfig
    spark: SparkConfig
    ingest: IngestConfig
    bronze: BronzeConfig
    silver: SilverConfig
    gold: GoldConfig
    quality: QualityConfig
    tuning: TuningConfig
    streaming: StreamingConfig
    iceberg: IcebergConfig


def _feeds(value) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ConfigError("[data] feeds must be a non empty list")
    for item in value:
        if item not in VALID_FEEDS:
            raise ConfigError(f"[data] feeds has unknown feed {item!r}, allowed {VALID_FEEDS}")
    if len(set(value)) != len(value):
        raise ConfigError("[data] feeds has duplicates")
    return tuple(value)


def _months(value, name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ConfigError(f"{name} must be a non empty list")
    return tuple(_as_month(item, name) for item in value)


def _strings(value, name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ConfigError(f"{name} must be a non empty list")
    return tuple(_as_str(item, name) for item in value)


def _data(table: dict, base: Path) -> DataConfig:
    root = Path(_as_str(_require(table, "root", "data"), "[data] root"))
    if not root.is_absolute():
        root = (base / root).resolve()
    start = _as_month(_require(table, "start_month", "data"), "[data] start_month")
    end = _as_month(_require(table, "end_month", "data"), "[data] end_month")
    if start > end:
        raise ConfigError(f"[data] start_month {start} is after end_month {end}")
    return DataConfig(
        root=root,
        feeds=_feeds(_require(table, "feeds", "data")),
        start_month=start,
        end_month=end,
        release_base=_as_url(_require(table, "release_base", "data"), "[data] release_base"),
        zone_lookup_url=_as_url(
            _require(table, "zone_lookup_url", "data"), "[data] zone_lookup_url"
        ),
    )


def _spark(table: dict) -> SparkConfig:
    return SparkConfig(
        master=_as_str(_require(table, "master", "spark"), "[spark] master"),
        app_name=_as_str(_require(table, "app_name", "spark"), "[spark] app_name"),
        driver_memory=_as_memory(
            _require(table, "driver_memory", "spark"), "[spark] driver_memory"
        ),
        executor_memory=_as_memory(
            _require(table, "executor_memory", "spark"), "[spark] executor_memory"
        ),
        executor_cores=_as_int(
            _require(table, "executor_cores", "spark"), "[spark] executor_cores", 1, 64
        ),
        shuffle_partitions=_as_int(
            _require(table, "shuffle_partitions", "spark"), "[spark] shuffle_partitions", 1, 20000
        ),
        adaptive=_as_bool(_require(table, "adaptive", "spark"), "[spark] adaptive"),
        adaptive_skew_join=_as_bool(
            _require(table, "adaptive_skew_join", "spark"), "[spark] adaptive_skew_join"
        ),
        broadcast_threshold_mb=_as_int(
            _require(table, "broadcast_threshold_mb", "spark"),
            "[spark] broadcast_threshold_mb",
            -1,
            8192,
        ),
        target_file_mb=_as_int(
            _require(table, "target_file_mb", "spark"), "[spark] target_file_mb", 1, 2048
        ),
        timezone=_as_str(_require(table, "timezone", "spark"), "[spark] timezone"),
        local_dir=str(table.get("local_dir", "")),
        log_level=_as_str(
            _require(table, "log_level", "spark"),
            "[spark] log_level",
            ("ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"),
        ),
        ui_enabled=_as_bool(_require(table, "ui_enabled", "spark"), "[spark] ui_enabled"),
    )


def _ingest(table: dict) -> IngestConfig:
    return IngestConfig(
        workers=_as_int(_require(table, "workers", "ingest"), "[ingest] workers", 1, 32),
        timeout_seconds=_as_int(
            _require(table, "timeout_seconds", "ingest"), "[ingest] timeout_seconds", 5, 7200
        ),
        retries=_as_int(_require(table, "retries", "ingest"), "[ingest] retries", 0, 10),
        chunk_kb=_as_int(_require(table, "chunk_kb", "ingest"), "[ingest] chunk_kb", 16, 16384),
        verify_header=_as_bool(
            _require(table, "verify_header", "ingest"), "[ingest] verify_header"
        ),
    )


def _bronze(table: dict) -> BronzeConfig:
    return BronzeConfig(
        timestamp_format=_as_str(
            _require(table, "timestamp_format", "bronze"), "[bronze] timestamp_format"
        ),
        rows_per_file=_as_int(
            _require(table, "rows_per_file", "bronze"), "[bronze] rows_per_file", 10000, 100000000
        ),
        keep_corrupt_rows=_as_bool(
            _require(table, "keep_corrupt_rows", "bronze"), "[bronze] keep_corrupt_rows"
        ),
    )


def _silver(table: dict) -> SilverConfig:
    minimum = _as_int(
        _require(table, "min_trip_seconds", "silver"), "[silver] min_trip_seconds", 0, 86400
    )
    maximum = _as_int(
        _require(table, "max_trip_seconds", "silver"), "[silver] max_trip_seconds", 1, 604800
    )
    if minimum >= maximum:
        raise ConfigError("[silver] min_trip_seconds must be below max_trip_seconds")
    return SilverConfig(
        min_trip_seconds=minimum,
        max_trip_seconds=maximum,
        max_trip_miles=_as_float(
            _require(table, "max_trip_miles", "silver"), "[silver] max_trip_miles", 0.1
        ),
        max_speed_mph=_as_float(
            _require(table, "max_speed_mph", "silver"), "[silver] max_speed_mph", 1.0
        ),
        max_fare=_as_float(_require(table, "max_fare", "silver"), "[silver] max_fare", 1.0),
        dedup=_as_bool(_require(table, "dedup", "silver"), "[silver] dedup"),
    )


def _gold(table: dict) -> GoldConfig:
    return GoldConfig(
        top_pairs=_as_int(_require(table, "top_pairs", "gold"), "[gold] top_pairs", 1, 100000),
        min_zone_trips=_as_int(
            _require(table, "min_zone_trips", "gold"), "[gold] min_zone_trips", 0, 10000000
        ),
    )


def _quality(table: dict) -> QualityConfig:
    return QualityConfig(
        max_corrupt_rate=_as_rate(
            _require(table, "max_corrupt_rate", "quality"), "[quality] max_corrupt_rate"
        ),
        max_null_pickup_rate=_as_rate(
            _require(table, "max_null_pickup_rate", "quality"), "[quality] max_null_pickup_rate"
        ),
        max_unknown_zone_rate=_as_rate(
            _require(table, "max_unknown_zone_rate", "quality"), "[quality] max_unknown_zone_rate"
        ),
        max_out_of_window_rate=_as_rate(
            _require(table, "max_out_of_window_rate", "quality"), "[quality] max_out_of_window_rate"
        ),
        min_rows_per_month=_as_int(
            _require(table, "min_rows_per_month", "quality"),
            "[quality] min_rows_per_month",
            0,
            100000000,
        ),
        fail_on=_as_str(_require(table, "fail_on", "quality"), "[quality] fail_on", VALID_SEVERITY),
    )


def _tuning(table: dict) -> TuningConfig:
    feed = _as_str(_require(table, "sample_feed", "tuning"), "[tuning] sample_feed", VALID_FEEDS)
    skew_feed = _as_str(_require(table, "skew_feed", "tuning"), "[tuning] skew_feed", VALID_FEEDS)
    return TuningConfig(
        sample_feed=feed,
        sample_months=_months(
            _require(table, "sample_months", "tuning"), "[tuning] sample_months"
        ),
        skew_feed=skew_feed,
        skew_months=_months(_require(table, "skew_months", "tuning"), "[tuning] skew_months"),
        experiments=_strings(_require(table, "experiments", "tuning"), "[tuning] experiments"),
        salt_buckets=_as_int(
            _require(table, "salt_buckets", "tuning"), "[tuning] salt_buckets", 2, 4096
        ),
        repeat=_as_int(_require(table, "repeat", "tuning"), "[tuning] repeat", 1, 20),
    )


def _streaming(table: dict) -> StreamingConfig:
    per_batch = _as_int(
        _require(table, "files_per_batch", "streaming"), "[streaming] files_per_batch", 1, 1000
    )
    produced = _as_int(
        _require(table, "producer_files", "streaming"), "[streaming] producer_files", 1, 1000
    )
    if produced % per_batch:
        raise ConfigError(
            "[streaming] producer_files must be a multiple of files_per_batch so the late file "
            "starts its own micro batch"
        )
    return StreamingConfig(
        files_per_batch=per_batch,
        trigger_seconds=_as_int(
            _require(table, "trigger_seconds", "streaming"), "[streaming] trigger_seconds", 0, 3600
        ),
        watermark_minutes=_as_int(
            _require(table, "watermark_minutes", "streaming"),
            "[streaming] watermark_minutes",
            1,
            10080,
        ),
        window_minutes=_as_int(
            _require(table, "window_minutes", "streaming"), "[streaming] window_minutes", 1, 1440
        ),
        producer_files=produced,
        producer_rows=_as_int(
            _require(table, "producer_rows", "streaming"),
            "[streaming] producer_rows",
            1000,
            100000000,
        ),
        late_rows=_as_int(
            _require(table, "late_rows", "streaming"), "[streaming] late_rows", 0, 100000000
        ),
        late_offset_hours=_as_int(
            _require(table, "late_offset_hours", "streaming"),
            "[streaming] late_offset_hours",
            1,
            8760,
        ),
    )


def _iceberg(table: dict) -> IcebergConfig:
    return IcebergConfig(
        catalog_name=_as_str(
            _require(table, "catalog_name", "iceberg"), "[iceberg] catalog_name"
        ),
        namespace=_as_str(_require(table, "namespace", "iceberg"), "[iceberg] namespace"),
        tables=_strings(_require(table, "tables", "iceberg"), "[iceberg] tables"),
    )


def load(path: str | Path = "config.toml") -> Config:
    path = Path(path).resolve()
    if not path.exists():
        raise ConfigError(f"config file not found: {path}")
    with path.open("rb") as handle:
        raw = tomllib.load(handle)
    missing = [
        name
        for name in (
            "data",
            "spark",
            "ingest",
            "bronze",
            "silver",
            "gold",
            "quality",
            "tuning",
            "streaming",
            "iceberg",
        )
        if name not in raw
    ]
    if missing:
        raise ConfigError(f"config is missing sections: {', '.join(missing)}")
    return Config(
        path=path,
        data=_data(raw["data"], path.parent),
        spark=_spark(raw["spark"]),
        ingest=_ingest(raw["ingest"]),
        bronze=_bronze(raw["bronze"]),
        silver=_silver(raw["silver"]),
        gold=_gold(raw["gold"]),
        quality=_quality(raw["quality"]),
        tuning=_tuning(raw["tuning"]),
        streaming=_streaming(raw["streaming"]),
        iceberg=_iceberg(raw["iceberg"]),
    )
