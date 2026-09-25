"""Raw readings -> hourly aggregates, with interchangeable engines (DuckDB, Spark).

Both engines run the same SQL (see `sql.py`), so a parity test can check they agree.
"""

from datetime import date
from pathlib import Path
from typing import Any


def run(engine: str, lake: Path, day: date) -> dict[str, Any]:
    if engine == "duckdb":
        from pipeline.transform import duckdb_engine

        return duckdb_engine.transform(lake, day)
    if engine == "spark":
        from pipeline.transform import spark_engine

        return spark_engine.transform(lake, day)
    raise ValueError(f"unknown engine {engine!r} (expected duckdb or spark)")
