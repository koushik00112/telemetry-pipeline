"""Run dbt as a subprocess and summarise its results.

A subprocess (not dbt's Python API) keeps dbt's pinned dependencies out of whatever
process calls us: in the Airflow image dbt lives in its own virtualenv (ADR 0001).
"""

import json
import os
import shutil
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from pipeline.config import Settings


def _dbt_bin(configured: str) -> str:
    if configured != "dbt" or shutil.which("dbt"):
        return configured
    beside = Path(sys.executable).parent / "dbt"  # the dbt installed in this virtualenv
    return str(beside) if beside.exists() else configured


def build(settings: Settings, select: str | None = None) -> dict[str, Any]:
    s = settings.resolved()
    if not (s.dbt_project / "dbt_project.yml").exists():
        raise FileNotFoundError(
            f"no dbt project at {s.dbt_project}; set DBT_PROJECT_DIR to the repo's dbt/ folder"
        )
    s.warehouse.parent.mkdir(parents=True, exist_ok=True)
    target = s.reports / "dbt" / "target"
    cmd = [
        _dbt_bin(s.dbt_bin),
        "build",
        "--project-dir",
        str(s.dbt_project),
        "--profiles-dir",
        str(s.dbt_project),
        "--target-path",
        str(target),
        "--log-path",
        str(s.reports / "dbt" / "logs"),
        "--vars",
        json.dumps({"lake": str(s.lake), "expected_per_hour": s.expected_per_hour}),
    ]
    if select:
        cmd += ["--select", select]
    env = {**os.environ, "WAREHOUSE_PATH": str(s.warehouse)}
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True)  # noqa: S603
    summary = summarise(target / "run_results.json")
    summary["returncode"] = proc.returncode
    if proc.returncode != 0:
        summary["output_tail"] = proc.stdout[-4000:] + proc.stderr[-2000:]
    return summary


def summarise(run_results: Path) -> dict[str, Any]:
    if not run_results.exists():
        return {"status": "error", "detail": "dbt produced no run_results.json"}
    results = json.loads(run_results.read_text())["results"]
    by_type: dict[str, Counter[str]] = {"model": Counter(), "test": Counter()}
    failures = []
    for r in results:
        kind = "test" if r["unique_id"].startswith("test.") else "model"
        by_type[kind][r["status"]] += 1
        if r["status"] in ("fail", "error", "warn"):
            failures.append(
                {
                    "node": r["unique_id"],
                    "status": r["status"],
                    "failures": r.get("failures"),
                    "message": r.get("message"),
                }
            )
    bad = by_type["test"]["fail"] + by_type["test"]["error"] + by_type["model"]["error"]
    return {
        "status": "fail" if bad else ("warn" if by_type["test"]["warn"] else "pass"),
        "models": dict(by_type["model"]),
        "tests": dict(by_type["test"]),
        "issues": failures,
    }
