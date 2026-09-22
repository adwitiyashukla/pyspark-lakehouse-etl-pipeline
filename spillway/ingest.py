from __future__ import annotations

import gzip
import hashlib
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import httpx

from . import schemas, sources, state
from .config import Config

LOG = logging.getLogger(__name__)
ZONE_FILE = "taxi_zone_lookup.csv"
ZONE_HEADER = ("LocationID", "Borough", "Zone")
EXPECTED_ZONES = 265


class IngestError(RuntimeError):
    pass


def _sha256_of(path: Path, chunk: int) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            block = handle.read(chunk)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def _first_line(path: Path) -> str:
    with gzip.open(path, "rt", encoding="utf-8", errors="replace") as handle:
        return handle.readline()


def _download(url: str, target: Path, timeout: int, chunk: int, retries: int) -> int:
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_suffix(target.suffix + ".part")
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            written = 0
            with httpx.stream("GET", url, timeout=timeout, follow_redirects=True) as response:
                if response.status_code == 404:
                    raise FileNotFoundError(url)
                response.raise_for_status()
                with partial.open("wb") as handle:
                    for block in response.iter_bytes(chunk):
                        handle.write(block)
                        written += len(block)
            partial.replace(target)
            return written
        except FileNotFoundError:
            partial.unlink(missing_ok=True)
            raise
        except Exception as error:
            last_error = error
            partial.unlink(missing_ok=True)
            if attempt < retries:
                time.sleep(2**attempt)
    raise IngestError(f"download failed for {url}: {last_error}")


def _ingest_one(config: Config, feed: str, month: str, force: bool) -> dict:
    url = sources.file_url(config.data.release_base, feed, month)
    target = sources.landing_path(config.data.landing, feed, month)
    started = time.perf_counter()
    record = {"feed": feed, "month": month, "url": url, "path": str(target)}
    if target.exists() and not force:
        record["_fetch"] = "cached"
        record["bytes"] = target.stat().st_size
    else:
        try:
            record["bytes"] = _download(
                url,
                target,
                config.ingest.timeout_seconds,
                config.ingest.chunk_kb * 1024,
                config.ingest.retries,
            )
        except FileNotFoundError:
            record["status"] = "missing"
            record["_fetch"] = "absent"
            record["detail"] = "not published in the mirror"
            record["seconds"] = round(time.perf_counter() - started, 3)
            return record
        record["_fetch"] = "downloaded"
    header = schemas.normalize_header(_first_line(target))
    record["header"] = ",".join(header)
    variant = schemas.resolve(feed, header)
    if variant is None and config.ingest.verify_header:
        record["status"] = "rejected"
        record["detail"] = "header does not match any known layout for this feed"
    else:
        record["status"] = "ready"
        record["variant"] = variant.name if variant else ""
    record["sha256"] = _sha256_of(target, config.ingest.chunk_kb * 1024)
    record["seconds"] = round(time.perf_counter() - started, 3)
    return record


def zone_lookup(config: Config, force: bool = False) -> Path:
    target = config.data.reference / ZONE_FILE
    if target.exists() and not force:
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    response = httpx.get(
        config.data.zone_lookup_url, timeout=config.ingest.timeout_seconds, follow_redirects=True
    )
    response.raise_for_status()
    text = response.text.replace("\r\n", "\n").replace("\r", "\n")
    lines = [line for line in text.split("\n") if line.strip()]
    header = schemas.normalize_header(lines[0])
    if header != ZONE_HEADER:
        raise IngestError(f"zone lookup header changed: {header}")
    if len(lines) - 1 != EXPECTED_ZONES:
        raise IngestError(f"zone lookup has {len(lines) - 1} rows, expected {EXPECTED_ZONES}")
    target.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return target


def run(config: Config, force: bool = False, feeds: tuple[str, ...] | None = None) -> dict:
    planned = [
        item for item in sources.planned_files(config) if feeds is None or item[0] in feeds
    ]
    LOG.info("ingest: %d month files planned", len(planned))
    zone_lookup(config, force)
    LOG.info("ingest: zone lookup ready")
    summary = {"ready": 0, "rejected": 0, "missing": 0, "bytes": 0, "downloaded": 0, "cached": 0}
    summary["recorded"] = 0
    done = 0
    with state.session(config.data.state_db) as conn:
        pending = []
        for feed, month in planned:
            known = state.file_status(conn, feed, month)
            landed = sources.landing_path(config.data.landing, feed, month)
            recorded = bool(known) and not force and known["status"] in (
                "ready",
                "rejected",
                "missing",
            )
            if recorded and (known["status"] != "ready" or landed.exists()):
                summary[known["status"]] = summary.get(known["status"], 0) + 1
                summary["recorded"] += 1
                summary["bytes"] += known["bytes"] or 0
                continue
            pending.append((feed, month))
        LOG.info(
            "ingest: %d files to fetch, %d already recorded",
            len(pending),
            len(planned) - len(pending),
        )
        if pending:
            with ThreadPoolExecutor(max_workers=config.ingest.workers) as pool:
                futures = {
                    pool.submit(_ingest_one, config, feed, month, force): (feed, month)
                    for feed, month in pending
                }
                for future in as_completed(futures):
                    feed, month = futures[future]
                    record = future.result()
                    fetch = record.pop("_fetch", "cached")
                    if fetch in ("downloaded", "cached"):
                        summary[fetch] += 1
                    status = record["status"]
                    summary[status] = summary.get(status, 0) + 1
                    summary["bytes"] += record.get("bytes") or 0
                    state.record_file(conn, **record)
                    done += 1
                    if status == "rejected":
                        LOG.warning(
                            "ingest: %s %s rejected, header %s",
                            feed,
                            month,
                            record["header"],
                        )
                    if done % 5 == 0 or done == len(pending):
                        LOG.info(
                            "ingest: %d/%d files, %.2f GB landed",
                            done,
                            len(pending),
                            summary["bytes"] / 1e9,
                        )
    LOG.info(
        "ingest done: %d ready, %d rejected, %d missing, %.2f GB",
        summary["ready"],
        summary["rejected"],
        summary["missing"],
        summary["bytes"] / 1e9,
    )
    return summary
