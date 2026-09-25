"""Runtime versus data size, per engine. Numbers are only meaningful with their context,
so the output records the machine, versions and exact row counts alongside the times.
"""

import json
import os
import platform
import shutil
import time
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from pipeline import generate, lake, quality, transform
from pipeline.config import Settings

BENCH_DAY = date(2026, 9, 1)


def _dir_bytes(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*.parquet"))


def _memory_gb() -> float | None:
    try:
        return round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 1e9, 1)
    except (ValueError, OSError, AttributeError):
        return None


def _versions() -> dict[str, str]:
    import duckdb
    import pyarrow

    out = {
        "python": platform.python_version(),
        "duckdb": duckdb.__version__,
        "pyarrow": pyarrow.__version__,
    }
    try:
        import pyspark

        out["pyspark"] = pyspark.__version__
    except ImportError:
        pass
    return out


def run(settings: Settings, scales: list[int], engines: list[str], out: str) -> dict[str, Any]:
    out_dir = Path(out)
    out_dir.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    for scale in scales:
        root = settings.lake.parent / f"bench-lake-x{scale}"
        shutil.rmtree(root, ignore_errors=True)
        t0 = time.perf_counter()
        injected = generate.write_day(root, BENCH_DAY, scale=scale)
        gen_s = time.perf_counter() - t0
        raw_bytes = _dir_bytes(lake.partition_dir(root, lake.RAW, BENCH_DAY))
        t0 = time.perf_counter()
        quality.check_raw(root, BENCH_DAY, settings.expected_per_hour)
        dq_s = time.perf_counter() - t0
        for engine in engines:
            # Spark: run twice and report the second run. The first run in the process also
            # starts the JVM, so for the first scale it includes ~5 s of startup.
            cold = None
            if engine == "spark":
                t0 = time.perf_counter()
                transform.run(engine, root, BENCH_DAY)
                cold = time.perf_counter() - t0
            t0 = time.perf_counter()
            result = transform.run(engine, root, BENCH_DAY)
            warm = time.perf_counter() - t0
            rows.append(
                {
                    "scale": scale,
                    "engine": engine,
                    "raw_rows": injected.rows_written,
                    "raw_mb": round(raw_bytes / 1e6, 1),
                    "curated_rows": result["rows"],
                    "generate_s": round(gen_s, 2),
                    "raw_quality_s": round(dq_s, 2),
                    "transform_s": round(warm, 2),
                    "transform_cold_s": round(cold, 2) if cold is not None else None,
                    "readings_per_s": int(injected.rows_written / warm),
                }
            )
        shutil.rmtree(root, ignore_errors=True)
    result = {
        "measured_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "machine": {
            "platform": platform.platform(),
            "processor": platform.processor(),
            "cpus": os.cpu_count(),
            "memory_gb": _memory_gb(),
            "spark_driver_memory": os.environ.get("SPARK_DRIVER_MEMORY", "4g"),
        },
        "versions": _versions(),
        "data": "SYNTHETIC (pipeline.generate), one UTC day per scale",
        "rows": rows,
    }
    (out_dir / "bench.json").write_text(json.dumps(result, indent=2))
    lines = [
        "# Benchmark: runtime vs data size",
        "",
        f"Measured {result['measured_at']} on {result['machine']['platform']}, "
        f"{result['machine']['cpus']} CPUs, {result['machine']['memory_gb']} GB RAM, "
        f"Spark driver memory {result['machine']['spark_driver_memory']}. "
        f"Versions: {result['versions']}.",
        "Data: synthetic, one day per scale (1x = 100 devices x 3 metrics x 1/min). "
        "Spark runs in local mode on the same machine; this is not a cluster benchmark.",
        "",
        "Transform time is the second of two runs for Spark (first run in brackets; only "
        "the first scale's first run includes JVM start-up).",
        "",
        "| Scale | Engine | Raw rows | Raw MB | Transform s | (Spark 1st run s) | Readings/s |",
        "|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        lines.append(
            f"| {r['scale']}x | {r['engine']} | {r['raw_rows']:,} | {r['raw_mb']} | "
            f"{r['transform_s']} | {r['transform_cold_s'] or '–'} | {r['readings_per_s']:,} |"
        )
    (out_dir / "bench.md").write_text("\n".join(lines) + "\n")
    return result
