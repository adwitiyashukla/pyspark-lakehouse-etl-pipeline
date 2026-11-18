from __future__ import annotations

from pathlib import Path

from .config import Config

FEED_TAGS = {"yellow": "yellow", "green": "green", "fhv": "fhv", "fhvhv": "fhvhv"}


def month_range(start: str, end: str) -> list[str]:
    year, month = (int(part) for part in start.split("-"))
    last_year, last_month = (int(part) for part in end.split("-"))
    out: list[str] = []
    while (year, month) <= (last_year, last_month):
        out.append(f"{year:04d}-{month:02d}")
        month += 1
        if month == 13:
            year, month = year + 1, 1
    return out


def next_month(month: str) -> str:
    year, index = (int(part) for part in month.split("-"))
    index += 1
    if index == 13:
        year, index = year + 1, 1
    return f"{year:04d}-{index:02d}"


def file_name(feed: str, month: str) -> str:
    return f"{feed}_tripdata_{month}.csv.gz"


def file_url(base: str, feed: str, month: str) -> str:
    return f"{base.rstrip('/')}/{FEED_TAGS[feed]}/{file_name(feed, month)}"


def landing_path(root: Path, feed: str, month: str) -> Path:
    return root / feed / file_name(feed, month)


def planned_files(config: Config) -> list[tuple[str, str]]:
    months = month_range(config.data.start_month, config.data.end_month)
    return [(feed, month) for feed in config.data.feeds for month in months]


def bronze_feed(root: Path, feed: str) -> Path:
    return root / feed


def bronze_partition(root: Path, feed: str, month: str) -> Path:
    return root / feed / f"file_month={month}"
