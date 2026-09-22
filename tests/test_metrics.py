from __future__ import annotations

from spillway import metrics

STAGES = [
    {
        "stageId": 1,
        "attemptId": 0,
        "status": "COMPLETE",
        "numCompleteTasks": 4,
        "executorRunTime": 8000,
        "shuffleReadBytes": 1000,
        "shuffleWriteBytes": 2000,
        "memoryBytesSpilled": 10,
        "diskBytesSpilled": 20,
        "inputBytes": 5000,
        "outputBytes": 100,
        "name": "old stage",
    },
    {
        "stageId": 2,
        "attemptId": 0,
        "status": "COMPLETE",
        "numCompleteTasks": 8,
        "executorRunTime": 4000,
        "shuffleReadBytes": 7,
        "shuffleWriteBytes": 9,
        "memoryBytesSpilled": 0,
        "diskBytesSpilled": 0,
        "inputBytes": 11,
        "outputBytes": 13,
        "name": "new stage",
    },
    {"stageId": 3, "attemptId": 0, "status": "SKIPPED", "numCompleteTasks": 99, "name": "skipped"},
]


class _Probe(metrics.Probe):
    def __init__(self):
        self.base = "http://localhost:4040"
        self.app_id = "app-1"
        self.calls = []

    def _fetch(self, path):
        self.calls.append(path)
        if path.endswith("/stages"):
            return STAGES
        return {"executorRunTime": [100, 900]}


def test_only_new_completed_stages_are_counted():
    probe = _Probe()
    result = probe.collect({(1, 0)}, wall=2.5)
    assert result.stages == 1
    assert result.tasks == 8
    assert result.shuffle_read_bytes == 7
    assert result.input_bytes == 11
    assert result.wall_seconds == 2.5
    assert result.detail["widest_stage"] == "new stage"


def test_task_quantiles_are_attached():
    probe = _Probe()
    result = probe.collect(set(), wall=1.0)
    assert result.median_task_ms == 100
    assert result.max_task_ms == 900
    assert result.tasks == 12


def test_measure_without_a_ui_still_times_the_action(spark):
    probe = metrics.Probe(spark)
    collected, value = metrics.measure(probe, lambda: 7)
    assert value == 7
    assert collected.wall_seconds >= 0
