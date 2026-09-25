"""Compose the steps: one day, a backfill over many days, and the idempotency proof."""

from datetime import date, timedelta
from pathlib import Path
from typing import Any

from pipeline import dbt_runner, extract, generate, lake, quality, transform
from pipeline.config import Settings


class QualityFailure(RuntimeError):
    pass


def ingest(
    settings: Settings, day: date, source: str, scale: int = 1, seed: int = 0
) -> dict[str, Any]:
    if source == "generate":
        injected = generate.write_day(settings.lake, day, scale=scale, seed=seed)
        return {"source": "synthetic", "rows": injected.rows_written}
    if source == "platform":
        if not settings.platform_database_url:
            raise SystemExit("PLATFORM_DATABASE_URL is not set")
        return extract.write_day(settings.lake, day, settings.platform_database_url)
    raise ValueError(f"unknown source {source!r}")


def check(settings: Settings, layer: str, day: date) -> dict[str, Any]:
    report = quality.run(settings.lake, settings.reports, layer, day, settings.expected_per_hour)
    if report["status"] == "fail":
        failed = [c["name"] for c in report["checks"] if c["status"] == "fail"]
        raise QualityFailure(f"{layer} quality checks failed for {day}: {failed}")
    return report


def run_day(
    settings: Settings,
    day: date,
    source: str = "generate",
    engine: str = "duckdb",
    scale: int = 1,
    seed: int = 0,
) -> dict[str, Any]:
    """Ingest -> check raw -> transform -> check curated, for one day."""
    return {
        "date": day.isoformat(),
        "ingest": ingest(settings, day, source, scale, seed),
        "quality_raw": check(settings, "raw", day)["status"],
        "transform": transform.run(engine, settings.lake, day),
        "quality_curated": check(settings, "curated", day)["status"],
    }


def days(start: date, end: date) -> list[date]:
    """Inclusive range."""
    if end < start:
        raise ValueError("end is before start")
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def backfill(settings: Settings, start: date, end: date, **kwargs: Any) -> dict[str, Any]:
    """Run every day in [start, end], then rebuild the dbt models once."""
    results = [run_day(settings, d, **kwargs) for d in days(start, end)]
    return {"days": results, "dbt": dbt_runner.build(settings)}


def snapshot(settings: Settings, day: date) -> dict[str, Any]:
    return {
        "raw": lake.fingerprint(settings.lake, lake.RAW, day),
        "curated": lake.fingerprint(settings.lake, lake.CURATED, day),
    }


def verify_idempotency(settings: Settings, day: date, **kwargs: Any) -> dict[str, Any]:
    """Run a day twice and compare content fingerprints of every layer it writes."""
    run_day(settings, day, **kwargs)
    first = snapshot(settings, day)
    run_day(settings, day, **kwargs)
    second = snapshot(settings, day)
    return {"date": day.isoformat(), "first": first, "second": second, "identical": first == second}


def partition_listing(lake_root: Path, dataset: str) -> list[str]:
    root = lake_root / dataset
    return sorted(p.name for p in root.glob("date=*") if p.is_dir()) if root.exists() else []
