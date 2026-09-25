"""The Parquet data lake: partition layout, atomic partition writes, content fingerprints.

Layout (Hive-style, one partition per UTC day):
  <lake>/raw/readings/date=YYYY-MM-DD/part-*.parquet
  <lake>/curated/hourly/date=YYYY-MM-DD/part-*.parquet

Every write replaces a whole partition atomically, so rerunning a day can never leave
a mix of old and new files. That's what makes each step idempotent.
"""

import dataclasses
import json
import shutil
import uuid
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa

RAW = "raw/readings"
CURATED = "curated/hourly"

RAW_SCHEMA = pa.schema(
    [
        ("device_id", pa.string()),
        ("metric", pa.string()),
        ("value", pa.float64()),
        ("ts", pa.timestamp("us", tz="UTC")),
    ]
)

CURATED_SCHEMA = pa.schema(
    [
        ("hour", pa.timestamp("us", tz="UTC")),
        ("device_id", pa.string()),
        ("metric", pa.string()),
        ("n_readings", pa.int64()),
        ("n_invalid", pa.int64()),
        ("n_duplicates", pa.int64()),
        ("mean", pa.float64()),
        ("min", pa.float64()),
        ("max", pa.float64()),
        ("stddev", pa.float64()),
        ("p95", pa.float64()),
    ]
)

# Columns that identify a row; used for fingerprints and reconciliation.
KEYS = {RAW: ("device_id", "metric", "ts"), CURATED: ("hour", "device_id", "metric")}


def partition_dir(lake: Path, dataset: str, day: date) -> Path:
    return lake / dataset / f"date={day.isoformat()}"


def partition_glob(lake: Path, dataset: str, day: date) -> str:
    return str(partition_dir(lake, dataset, day) / "*.parquet")


def sql_str(value: str) -> str:
    """Quote a string as a SQL literal. Used for file paths where DuckDB can't take a
    bound parameter (e.g. inside CREATE VIEW)."""
    return "'" + value.replace("'", "''") + "'"


def read_sql(lake: Path, dataset: str, day: date) -> str:
    return f"read_parquet({sql_str(partition_glob(lake, dataset, day))})"


def exists(lake: Path, dataset: str, day: date) -> bool:
    return any(partition_dir(lake, dataset, day).glob("*.parquet"))


def replace_partition(
    lake: Path, dataset: str, day: date, write: Callable[[Path], None], manifest: dict[str, Any]
) -> Path:
    """Write a partition into a temporary sibling directory, then swap it in.

    `write` receives an empty directory and must put the partition's files in it.
    Readers see either the old complete partition or the new complete one, never a mix.
    """
    final = partition_dir(lake, dataset, day)
    final.parent.mkdir(parents=True, exist_ok=True)
    tmp = final.parent / f".tmp-{final.name}-{uuid.uuid4().hex[:8]}"
    tmp.mkdir()
    try:
        write(tmp)
        (tmp / "_manifest.json").write_text(json.dumps(manifest, indent=2, default=_json))
        old = None
        if final.exists():
            old = final.parent / f".old-{final.name}-{uuid.uuid4().hex[:8]}"
            final.rename(old)
        tmp.rename(final)
        if old is not None:
            shutil.rmtree(old)
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    return final


def _json(obj: Any) -> Any:
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return dataclasses.asdict(obj)
    return str(obj)


def read_manifest(lake: Path, dataset: str, day: date) -> dict[str, Any]:
    path = partition_dir(lake, dataset, day) / "_manifest.json"
    return json.loads(path.read_text()) if path.exists() else {}


def fingerprint(lake: Path, dataset: str, day: date) -> dict[str, Any]:
    """Order-independent content hash of a partition, plus its row count.

    Rows are hashed individually and summed, so file boundaries and row order don't matter.
    Floats are rounded to 9 decimals first: parallel aggregation can change the last bits
    of a sum between runs without the data having changed.
    """
    con = duckdb.connect()
    cols = ", ".join(
        f"round({c}, 9)" if c in ("value", "mean", "min", "max", "stddev", "p95") else c
        for c in RAW_SCHEMA.names + CURATED_SCHEMA.names
        if c in _columns(con, lake, dataset, day)
    )
    rows, digest = con.execute(
        f"SELECT count(*), coalesce(sum(hash({cols}))::VARCHAR, '0') "
        f"FROM {read_sql(lake, dataset, day)}"
    ).fetchone() or (0, "0")
    return {"rows": int(rows), "hash": str(digest)}


def _columns(con: duckdb.DuckDBPyConnection, lake: Path, dataset: str, day: date) -> set[str]:
    rel = con.sql(f"SELECT * FROM {read_sql(lake, dataset, day)} LIMIT 0")
    return set(rel.columns)
