import time
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq

from pipeline import lake
from pipeline.transform.sql import hourly


def transform(lake_root: Path, day: date) -> dict[str, Any]:
    if not lake.exists(lake_root, lake.RAW, day):
        raise FileNotFoundError(f"no raw partition for {day}; run ingest first")
    started = time.perf_counter()
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute(f"CREATE VIEW raw AS SELECT * FROM {lake.read_sql(lake_root, lake.RAW, day)}")
    table = con.execute(hourly("duckdb")).to_arrow_table().cast(lake.CURATED_SCHEMA)

    def write(tmp: Path) -> None:
        pq.write_table(table, tmp / "part-00000.parquet", compression="zstd")

    elapsed = time.perf_counter() - started
    manifest = {"engine": "duckdb", "rows": table.num_rows, "seconds": round(elapsed, 3)}
    lake.replace_partition(lake_root, lake.CURATED, day, write, manifest)
    return manifest
