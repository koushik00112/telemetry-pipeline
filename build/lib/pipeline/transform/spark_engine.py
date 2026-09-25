"""Spark engine (local mode). Same SQL as DuckDB; see sql.py.

Local mode on one machine: this shows the code and the scaling behaviour, not a cluster.
"""

import os
import shutil
import time
from datetime import date
from pathlib import Path
from typing import Any

from pipeline import lake
from pipeline.transform.sql import hourly

_spark: Any = None


def session() -> Any:
    global _spark
    if _spark is None:
        from pyspark.sql import SparkSession

        _spark = (
            SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[*]"))
            .appName("telemetry-pipeline")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            # Local mode runs everything in the driver JVM; the 1 GB default runs out at 100x.
            .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
            .config(
                "spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "32")
            )
            .config("spark.sql.adaptive.enabled", "true")
            # Default in Spark 3 is INT96, which other readers treat inconsistently.
            .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
            .getOrCreate()
        )
        _spark.sparkContext.setLogLevel("ERROR")
    return _spark


def transform(lake_root: Path, day: date) -> dict[str, Any]:
    if not lake.exists(lake_root, lake.RAW, day):
        raise FileNotFoundError(f"no raw partition for {day}; run ingest first")
    spark = session()
    started = time.perf_counter()
    raw = spark.read.parquet(str(lake.partition_dir(lake_root, lake.RAW, day)))
    raw.createOrReplaceTempView("raw")
    result = spark.sql(hourly("spark"))
    rows = 0

    def write(tmp: Path) -> None:
        nonlocal rows
        out = tmp / "_spark"
        result.coalesce(1).write.mode("overwrite").parquet(str(out))
        # Move Spark's part file up and drop its _SUCCESS/.crc bookkeeping files.
        for i, part in enumerate(sorted(out.glob("part-*.parquet"))):
            part.rename(tmp / f"part-{i:05d}.parquet")
        shutil.rmtree(out)
        rows = spark.read.parquet(str(tmp)).count()

    lake.replace_partition(lake_root, lake.CURATED, day, write, {"engine": "spark"})
    elapsed = time.perf_counter() - started
    return {"engine": "spark", "rows": rows, "seconds": round(elapsed, 3)}
