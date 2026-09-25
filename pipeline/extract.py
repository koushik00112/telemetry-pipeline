"""Land one UTC day of readings from the telemetry platform's database.

Reads the platform's `readings` table (Postgres in production, SQLite in tests) for
[day 00:00, next day 00:00) in chunks, and writes the raw partition atomically.
Rerunning a day re-reads it and replaces the partition, which is idempotent as long as
the source rows for that day haven't changed.

Written against SQLAlchemy Core APIs that exist in both 1.4 and 2.0.
"""

import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import sqlalchemy as sa

from pipeline import lake

CHUNK_ROWS = 500_000

_readings = sa.Table(
    "readings",
    sa.MetaData(),
    sa.Column("device_id", sa.String),
    sa.Column("metric", sa.String),
    sa.Column("value", sa.Float),
    sa.Column("ts", sa.DateTime(timezone=True)),
)


def _uuid_text(value: Any) -> str:
    # Postgres returns uuid.UUID; SQLAlchemy's Uuid type on SQLite stores 32 hex chars.
    return str(value if isinstance(value, uuid.UUID) else uuid.UUID(str(value)))


def _utc(ts: datetime) -> datetime:
    return ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts.astimezone(UTC)


def write_day(lake_root: Path, day: date, database_url: str) -> dict[str, Any]:
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    end = start + timedelta(days=1)
    engine = sa.create_engine(database_url)
    stmt = (
        sa.select(_readings.c.device_id, _readings.c.metric, _readings.c.value, _readings.c.ts)
        .where(_readings.c.ts >= start, _readings.c.ts < end)
        .order_by(_readings.c.ts)
    )
    rows_total = 0

    def write(tmp: Path) -> None:
        nonlocal rows_total
        with engine.connect() as conn:
            result = conn.execution_options(stream_results=True).execute(stmt)
            part = 0
            while batch := result.fetchmany(CHUNK_ROWS):
                table = pa.table(
                    {
                        "device_id": [_uuid_text(r[0]) for r in batch],
                        "metric": [r[1] for r in batch],
                        "value": pa.array([r[2] for r in batch], type=pa.float64()),
                        "ts": pa.array(
                            [_utc(r[3]) for r in batch], type=pa.timestamp("us", tz="UTC")
                        ),
                    }
                ).cast(lake.RAW_SCHEMA)
                pq.write_table(table, tmp / f"part-{part:05d}.parquet", compression="zstd")
                rows_total += table.num_rows
                part += 1
            if part == 0:
                # An empty day is still a completed day: write an empty partition file.
                pq.write_table(lake.RAW_SCHEMA.empty_table(), tmp / "part-00000.parquet")

    lake.replace_partition(
        lake_root,
        lake.RAW,
        day,
        write,
        manifest={
            "source": "platform",
            "database": engine.url.render_as_string(hide_password=True),
            "window": [start.isoformat(), end.isoformat()],
        },
    )
    engine.dispose()
    return {"source": "platform", "rows": rows_total}
