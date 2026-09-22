from __future__ import annotations

import gzip

import pytest

from spillway import ingest, schemas, sources


class _Response:
    def __init__(self, text: str):
        self.text = text

    def raise_for_status(self):
        return None


def _zone_csv(rows: int) -> str:
    body = ["LocationID,Borough,Zone"]
    body.extend(f"{index},Manhattan,Zone {index}" for index in range(1, rows + 1))
    return "\r\n".join(body)


def test_zone_lookup_accepts_the_published_file(settings, monkeypatch):
    monkeypatch.setattr(ingest.httpx, "get", lambda *a, **k: _Response(_zone_csv(265)))
    target = ingest.zone_lookup(settings, force=True)
    lines = target.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 266
    assert "\r" not in target.read_text(encoding="utf-8")


def test_zone_lookup_rejects_a_short_file(settings, monkeypatch):
    monkeypatch.setattr(ingest.httpx, "get", lambda *a, **k: _Response(_zone_csv(12)))
    with pytest.raises(ingest.IngestError) as error:
        ingest.zone_lookup(settings, force=True)
    assert "265" in str(error.value)


def test_first_line_and_digest(tmp_path):
    path = tmp_path / "sample.csv.gz"
    with gzip.open(path, "wt", encoding="utf-8", newline="") as handle:
        handle.write("a,b\r\n1,2\r\n")
    assert schemas.normalize_header(ingest._first_line(path)) == ("a", "b")
    assert len(ingest._sha256_of(path, 1024)) == 64


def test_a_mislabeled_file_is_rejected(settings, monkeypatch, tmp_path):
    header = ",".join(schemas.FHV_V2.header)

    def fake_download(url, target, timeout, chunk, retries):
        target.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(target, "wt", encoding="utf-8", newline="") as handle:
            handle.write(header + "\r\nB1,2021-06-01 00:00:00,2021-06-01 00:10:00,1,2,,B1\r\n")
        return target.stat().st_size

    monkeypatch.setattr(ingest, "_download", fake_download)
    record = ingest._ingest_one(settings, "fhvhv", "2021-06", force=True)
    assert record["status"] == "rejected"
    assert record["header"] == header
    assert "header" in record["detail"]


def test_a_matching_file_is_accepted(settings, monkeypatch):
    header = ",".join(schemas.FHVHV_V1.header)

    def fake_download(url, target, timeout, chunk, retries):
        target.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(target, "wt", encoding="utf-8", newline="") as handle:
            handle.write(header + "\r\nHV0003,B1,2021-01-01 00:00:00,2021-01-01 00:20:00,1,2,\r\n")
        return target.stat().st_size

    monkeypatch.setattr(ingest, "_download", fake_download)
    record = ingest._ingest_one(settings, "fhvhv", "2021-01", force=True)
    assert record["status"] == "ready"
    assert record["variant"] == "fhvhv_v1"
    assert sources.landing_path(settings.data.landing, "fhvhv", "2021-01").exists()
