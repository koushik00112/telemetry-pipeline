import os
from dataclasses import dataclass, field
from pathlib import Path

# Physically plausible ranges. Values outside (or NaN) are counted as invalid and left out
# of aggregates. Metrics not listed here are only checked for NaN.
METRIC_RANGES: dict[str, tuple[float, float]] = {
    "temperature_c": (-40.0, 125.0),
    "humidity_pct": (0.0, 100.0),
    "vibration_rms": (0.0, 50.0),
}


@dataclass(frozen=True)
class Settings:
    lake: Path = field(default_factory=lambda: Path(os.environ.get("LAKE_ROOT", "lake")))
    warehouse: Path = field(
        default_factory=lambda: Path(os.environ.get("WAREHOUSE_PATH", "warehouse/telemetry.duckdb"))
    )
    reports: Path = field(default_factory=lambda: Path(os.environ.get("REPORTS_ROOT", "reports")))
    dbt_project: Path = field(
        default_factory=lambda: Path(
            os.environ.get("DBT_PROJECT_DIR", Path(__file__).parent.parent / "dbt")
        )
    )
    dbt_bin: str = field(default_factory=lambda: os.environ.get("DBT_BIN", "dbt"))
    platform_database_url: str | None = field(
        default_factory=lambda: os.environ.get("PLATFORM_DATABASE_URL")
    )
    # Readings each device should send per metric per hour (the simulator's 1/minute).
    expected_per_hour: int = field(
        default_factory=lambda: int(os.environ.get("EXPECTED_PER_HOUR", "60"))
    )

    def resolved(self) -> "Settings":
        return Settings(
            lake=self.lake.resolve(),
            warehouse=self.warehouse.resolve(),
            reports=self.reports.resolve(),
            dbt_project=self.dbt_project.resolve(),
            dbt_bin=self.dbt_bin,
            platform_database_url=self.platform_database_url,
            expected_per_hour=self.expected_per_hour,
        )
