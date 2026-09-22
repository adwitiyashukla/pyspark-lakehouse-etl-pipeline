from __future__ import annotations

import json
import logging
import time
import urllib.request
from dataclasses import dataclass, field

from pyspark.sql import SparkSession

LOG = logging.getLogger(__name__)
TIMEOUT = 20


@dataclass
class StageMetrics:
    wall_seconds: float = 0.0
    tasks: int = 0
    executor_seconds: float = 0.0
    shuffle_read_bytes: int = 0
    shuffle_write_bytes: int = 0
    spill_memory_bytes: int = 0
    spill_disk_bytes: int = 0
    input_bytes: int = 0
    output_bytes: int = 0
    max_task_ms: int = 0
    median_task_ms: int = 0
    stages: int = 0
    detail: dict = field(default_factory=dict)


class Probe:
    def __init__(self, spark: SparkSession):
        self.base = spark.sparkContext.uiWebUrl
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        self.app_id = self._app_id()

    def _fetch(self, path: str):
        if not self.base:
            return None
        try:
            with self.opener.open(f"{self.base}{path}", timeout=TIMEOUT) as response:
                return json.loads(response.read().decode("utf-8"))
        except Exception as error:
            LOG.debug("metrics fetch failed for %s: %s", path, error)
            return None

    def _app_id(self) -> str | None:
        payload = self._fetch("/api/v1/applications")
        if not payload:
            return None
        return payload[0]["id"]

    def available(self) -> bool:
        return self.app_id is not None

    def stage_ids(self) -> set[tuple[int, int]]:
        payload = self._fetch(f"/api/v1/applications/{self.app_id}/stages") or []
        return {(item["stageId"], item["attemptId"]) for item in payload}

    def _task_quantiles(self, stage_id: int, attempt: int) -> tuple[int, int]:
        payload = self._fetch(
            f"/api/v1/applications/{self.app_id}/stages/{stage_id}/{attempt}"
            "/taskSummary?quantiles=0.5,1.0"
        )
        if not payload or "executorRunTime" not in payload:
            return 0, 0
        values = payload["executorRunTime"]
        return int(values[0]), int(values[-1])

    def collect(self, before: set[tuple[int, int]], wall: float) -> StageMetrics:
        metrics = StageMetrics(wall_seconds=round(wall, 3))
        payload = self._fetch(f"/api/v1/applications/{self.app_id}/stages") or []
        fresh = [
            item
            for item in payload
            if (item["stageId"], item["attemptId"]) not in before
            and item.get("status") == "COMPLETE"
        ]
        biggest = None
        for item in fresh:
            metrics.stages += 1
            metrics.tasks += int(item.get("numCompleteTasks", 0) or 0)
            metrics.executor_seconds += float(item.get("executorRunTime", 0) or 0) / 1000.0
            metrics.shuffle_read_bytes += int(item.get("shuffleReadBytes", 0) or 0)
            metrics.shuffle_write_bytes += int(item.get("shuffleWriteBytes", 0) or 0)
            metrics.spill_memory_bytes += int(item.get("memoryBytesSpilled", 0) or 0)
            metrics.spill_disk_bytes += int(item.get("diskBytesSpilled", 0) or 0)
            metrics.input_bytes += int(item.get("inputBytes", 0) or 0)
            metrics.output_bytes += int(item.get("outputBytes", 0) or 0)
            complete = item.get("numCompleteTasks", 0)
            if biggest is None or complete > biggest.get("numCompleteTasks", 0):
                biggest = item
        if biggest is not None:
            median, maximum = self._task_quantiles(biggest["stageId"], biggest["attemptId"])
            metrics.median_task_ms = median
            metrics.max_task_ms = maximum
            metrics.detail["widest_stage"] = biggest.get("name", "")
            metrics.detail["widest_stage_tasks"] = int(biggest.get("numCompleteTasks", 0) or 0)
        return metrics


def measure(probe: Probe, action):
    before = probe.stage_ids() if probe.available() else set()
    started = time.perf_counter()
    result = action()
    wall = time.perf_counter() - started
    if not probe.available():
        return StageMetrics(wall_seconds=round(wall, 3)), result
    return probe.collect(before, wall), result
