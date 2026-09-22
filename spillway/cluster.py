from __future__ import annotations

import json
import logging
import os
import platform
import socket
import subprocess
import sys
import time
from pathlib import Path

import pyspark

from .config import Config

LOG = logging.getLogger(__name__)
MASTER_CLASS = "org.apache.spark.deploy.master.Master"
WORKER_CLASS = "org.apache.spark.deploy.worker.Worker"
MASTER_PORT = 7077
UI_PORT = 8080


def spark_home() -> Path:
    return Path(pyspark.__file__).parent


def spark_class() -> Path:
    home = spark_home()
    name = "spark-class2.cmd" if platform.system() == "Windows" else "spark-class"
    return home / "bin" / name


def state_file(config: Config) -> Path:
    return config.data.root / "cluster.json"


def _host() -> str:
    return socket.gethostbyname(socket.gethostname())


def _launch(args: list[str], log_path: Path) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    environment = dict(os.environ)
    environment.setdefault("SPARK_HOME", str(spark_home()))
    environment.setdefault("PYSPARK_PYTHON", sys.executable)
    environment.pop("PYSPARK_SUBMIT_ARGS", None)
    with log_path.open("ab") as handle:
        process = subprocess.Popen(
            args, stdout=handle, stderr=handle, env=environment, start_new_session=True
        )
    return process.pid


def start(config: Config, workers: int = 2, worker_cores: int = 1, worker_memory: str = "") -> dict:
    launcher = spark_class()
    if not launcher.exists():
        raise RuntimeError(f"spark-class not found at {launcher}")
    host = _host()
    logs = config.data.root / "logs"
    master_url = f"spark://{host}:{MASTER_PORT}"
    pids = {
        "master": _launch(
            [str(launcher), MASTER_CLASS, "--host", host, "--port", str(MASTER_PORT),
             "--webui-port", str(UI_PORT)],
            logs / "master.log",
        )
    }
    time.sleep(8)
    memory = worker_memory or config.spark.executor_memory
    pids["workers"] = []
    for index in range(workers):
        pids["workers"].append(
            _launch(
                [str(launcher), WORKER_CLASS, master_url, "--cores", str(worker_cores),
                 "--memory", memory, "--webui-port", str(UI_PORT + 1 + index)],
                logs / f"worker-{index}.log",
            )
        )
    time.sleep(6)
    payload = {
        "master_url": master_url,
        "master_ui": f"http://{host}:{UI_PORT}",
        "workers": workers,
        "worker_cores": worker_cores,
        "worker_memory": memory,
        "pids": pids,
    }
    state_file(config).parent.mkdir(parents=True, exist_ok=True)
    state_file(config).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    LOG.info("cluster: master at %s with %d workers", master_url, workers)
    return payload


def stop(config: Config) -> dict:
    path = state_file(config)
    if not path.exists():
        return {"stopped": 0}
    payload = json.loads(path.read_text(encoding="utf-8"))
    stopped = 0
    targets = [payload["pids"]["master"], *payload["pids"].get("workers", [])]
    for pid in targets:
        try:
            if platform.system() == "Windows":
                subprocess.run(
                ["taskkill", "/F", "/PID", str(pid)], check=False, capture_output=True
            )
            else:
                os.kill(pid, 15)
            stopped += 1
        except ProcessLookupError:
            continue
    path.unlink(missing_ok=True)
    LOG.info("cluster: stopped %d processes", stopped)
    return {"stopped": stopped}


def master_url(config: Config) -> str | None:
    path = state_file(config)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))["master_url"]
