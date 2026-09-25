"""Data-quality checks per partition, written as a JSON + Markdown report.

Each check has a status: pass, warn (worth a look; the pipeline continues) or fail
(the pipeline stops, because the data would mislead whoever uses it downstream).

The curated checks reconcile against raw: every raw reading must be accounted for
exactly once, as valid, invalid or duplicate. That catches silent row loss in the transform.
"""

import json
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb

from pipeline import lake
from pipeline.config import METRIC_RANGES
from pipeline.transform.sql import _invalid_expr


@dataclass
class Check:
    name: str
    status: str  # pass | warn | fail
    value: float | int | str
    threshold: str
    detail: str = ""


def _status(ok: bool, severity: str = "fail") -> str:
    return "pass" if ok else severity


def _con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    return con


def check_raw(lake_root: Path, day: date, expected_per_hour: int) -> list[Check]:
    con = _con()
    con.execute(f"CREATE VIEW raw AS SELECT * FROM {lake.read_sql(lake_root, lake.RAW, day)}")
    lo = datetime(day.year, day.month, day.day, tzinfo=UTC)
    hi = lo + timedelta(days=1)
    q = con.execute(
        """
        SELECT count(*) AS rows,
               count(*) FILTER (WHERE device_id IS NULL OR metric IS NULL OR ts IS NULL
                                OR value IS NULL) AS nulls,
               count(*) FILTER (WHERE ts < ? OR ts >= ?) AS outside_day,
               count(DISTINCT device_id) AS devices,
               count(DISTINCT metric) AS metrics
        FROM raw""",
        [lo, hi],
    ).fetchone()
    assert q is not None
    rows, nulls, outside, devices, metrics = q
    dup = con.execute(
        "SELECT count(*) - count(DISTINCT (device_id, metric, ts)) FROM raw"
    ).fetchone()
    duplicates = int(dup[0]) if dup else 0
    inv = con.execute(f"""
        SELECT count(*) FILTER (WHERE {_invalid_expr()}),
               count(*) FILTER (WHERE isnan(value)),
               count(*)
        FROM (SELECT DISTINCT device_id, metric, ts, value FROM raw)""").fetchone()
    assert inv is not None
    invalid, nans, distinct_rows = int(inv[0]), int(inv[1]), int(inv[2])
    unknown = [
        m
        for (m,) in con.execute("SELECT DISTINCT metric FROM raw").fetchall()
        if m not in METRIC_RANGES
    ]
    # Completeness per device and metric against the expected cadence.
    comp = con.execute(f"""
        SELECT quantile_cont(c, 0.05), min(c) FROM (
            SELECT count(DISTINCT ts) / {24 * expected_per_hour}.0 AS c
            FROM raw GROUP BY device_id, metric)""").fetchone()
    p5, worst = (float(comp[0]), float(comp[1])) if comp and comp[0] is not None else (0.0, 0.0)

    dup_rate = duplicates / rows if rows else 0.0
    inv_rate = invalid / distinct_rows if distinct_rows else 0.0
    return [
        Check("row_count", _status(rows > 0), int(rows), "> 0"),
        Check("null_fields", _status(nulls == 0), int(nulls), "= 0"),
        Check(
            "timestamps_outside_partition_day",
            _status(outside == 0),
            int(outside),
            "= 0",
            "readings stamped outside the UTC day they're filed under",
        ),
        Check(
            "duplicate_rate",
            _status(dup_rate < 0.01, "warn"),
            round(dup_rate, 6),
            "< 1%",
            f"{duplicates} duplicate rows",
        ),
        Check(
            "invalid_value_rate",
            _status(inv_rate < 0.01, "warn"),
            round(inv_rate, 6),
            "< 1%",
            f"{invalid} invalid ({nans} NaN, {invalid - nans} out of range)",
        ),
        Check(
            "unknown_metrics", _status(not unknown, "warn"), len(unknown), "= 0", ", ".join(unknown)
        ),
        Check(
            "completeness_p5",
            _status(p5 >= 0.8, "warn"),
            round(p5, 4),
            ">= 0.80",
            f"5th percentile over device-metrics; worst {worst:.2%}",
        ),
        Check("devices_reporting", "pass", int(devices), "info", f"{metrics} metrics"),
    ]


def check_curated(lake_root: Path, day: date) -> list[Check]:
    con = _con()
    con.execute(f"CREATE VIEW raw AS SELECT * FROM {lake.read_sql(lake_root, lake.RAW, day)}")
    con.execute(f"CREATE VIEW cur AS SELECT * FROM {lake.read_sql(lake_root, lake.CURATED, day)}")
    lo = datetime(day.year, day.month, day.day, tzinfo=UTC)
    raw = con.execute(
        "SELECT count(*), count(DISTINCT (device_id, metric, ts)) FROM raw"
    ).fetchone()
    cur = con.execute(
        """
        SELECT count(*), count(DISTINCT (hour, device_id, metric)),
               sum(n_readings + n_invalid), sum(n_readings + n_invalid + n_duplicates),
               count(*) FILTER (WHERE n_readings > 0 AND NOT (min <= mean AND mean <= max)),
               count(*) FILTER (WHERE hour < ? OR hour >= ? + INTERVAL 1 DAY)
        FROM cur""",
        [lo, lo],
    ).fetchone()
    assert raw is not None and cur is not None
    raw_rows, raw_keys = int(raw[0]), int(raw[1])
    rows, keys, accounted_keys, accounted_rows, bad_stats, bad_hours = (int(x or 0) for x in cur)
    return [
        Check("row_count", _status(rows > 0), rows, "> 0"),
        Check("unique_hour_device_metric", _status(rows == keys), rows - keys, "= 0 duplicates"),
        Check(
            "reconcile_distinct_readings",
            _status(accounted_keys == raw_keys),
            accounted_keys - raw_keys,
            "= 0",
            f"curated valid+invalid {accounted_keys} vs raw distinct keys {raw_keys}",
        ),
        Check(
            "reconcile_all_rows",
            _status(accounted_rows == raw_rows),
            accounted_rows - raw_rows,
            "= 0",
            f"curated valid+invalid+duplicates {accounted_rows} vs raw rows {raw_rows}",
        ),
        Check("mean_between_min_and_max", _status(bad_stats == 0), bad_stats, "= 0"),
        Check("hours_inside_partition_day", _status(bad_hours == 0), bad_hours, "= 0"),
    ]


def run(
    lake_root: Path, reports: Path, layer: str, day: date, expected_per_hour: int = 60
) -> dict[str, Any]:
    checks = (
        check_raw(lake_root, day, expected_per_hour)
        if layer == "raw"
        else check_curated(lake_root, day)
    )
    worst = (
        "fail"
        if any(c.status == "fail" for c in checks)
        else ("warn" if any(c.status == "warn" for c in checks) else "pass")
    )
    report = {
        "layer": layer,
        "date": day.isoformat(),
        "status": worst,
        "checked_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "checks": [asdict(c) for c in checks],
    }
    out = reports / "quality" / f"date={day.isoformat()}"
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{layer}.json").write_text(json.dumps(report, indent=2))
    lines = [
        f"# Data quality: {layer}, {day} — **{worst.upper()}**",
        "",
        "| Check | Status | Value | Threshold | Detail |",
        "|---|---|---|---|---|",
    ]
    lines += [f"| {c.name} | {c.status} | {c.value} | {c.threshold} | {c.detail} |" for c in checks]
    (out / f"{layer}.md").write_text("\n".join(lines) + "\n")
    return report
