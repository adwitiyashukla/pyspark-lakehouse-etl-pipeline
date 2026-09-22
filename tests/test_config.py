from __future__ import annotations

from pathlib import Path

import pytest

from spillway import config as config_module


def _write(tmp_path, body: str):
    target = tmp_path / "config.toml"
    target.write_text(body, encoding="utf-8")
    return target


def test_loads_the_shipped_config():
    settings = config_module.load("config.toml")
    assert settings.data.feeds
    assert settings.spark.shuffle_partitions > 0
    assert settings.data.bronze.name == "bronze"
    assert settings.data.root.is_absolute()


def test_ci_config_points_at_its_own_root():
    settings = config_module.load("config.ci.toml")
    assert settings.data.root.name == "data-ci"
    assert settings.data.start_month <= settings.data.end_month


def test_missing_section(tmp_path):
    with pytest.raises(config_module.ConfigError) as error:
        config_module.load(_write(tmp_path, "[data]\nroot = 'x'\n"))
    assert "missing sections" in str(error.value)


def test_missing_file(tmp_path):
    with pytest.raises(config_module.ConfigError):
        config_module.load(tmp_path / "absent.toml")


@pytest.mark.parametrize(
    "old,new,message",
    [
        ('feeds = ["yellow", "green", "fhv", "fhvhv"]', 'feeds = ["taxi"]', "unknown feed"),
        ('start_month = "2019-01"', 'start_month = "2022-13"', "outside 01 to 12"),
        ('driver_memory = "4g"', 'driver_memory = "4gb"', "4g or 512m"),
        ("shuffle_partitions = 48", "shuffle_partitions = 0", "at least 1"),
        ('fail_on = "error"', 'fail_on = "panic"', "must be one of"),
        ("max_corrupt_rate = 0.001", "max_corrupt_rate = 4.0", "rate between 0 and 1"),
        ("adaptive = true", 'adaptive = "yes"', "true or false"),
        ('end_month = "2021-07"', 'end_month = "2018-01"', "is after end_month"),
        ("min_trip_seconds = 30", "min_trip_seconds = 30000", "below max_trip_seconds"),
        (
            'release_base = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"',
            'release_base = "http://example.com"',
            "must start with https",
        ),
    ],
)
def test_validation_rejects_bad_values(tmp_path, old, new, message):
    body = Path("config.toml").read_text(encoding="utf-8").replace(old, new)
    with pytest.raises(config_module.ConfigError) as error:
        config_module.load(_write(tmp_path, body))
    assert message in str(error.value)


def test_relative_root_resolves_next_to_the_config(tmp_path):
    body = Path("config.toml").read_text(encoding="utf-8")
    settings = config_module.load(_write(tmp_path, body))
    assert settings.data.root == (tmp_path / "data").resolve()
