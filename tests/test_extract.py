"""Extract from a database shaped like the telemetry platform's `readings` table."""

import uuid
from datetime import UTC, datetime, timedelta

import pyarrow.parquet as pq
import sqlalchemy as sa

from pipeline import extract, lake, runner
from tests.conftest import DAY


def platform_db(path, rows):
    engine = sa.create_engine(f"sqlite:///{path}")
    with engine.begin() as c:
        c.execute(
            sa.text("""CREATE TABLE readings (id INTEGER PRIMARY KEY, device_id CHAR(32),
            metric VARCHAR(64), value FLOAT, ts DATETIME, received_at DATETIME, alert_checked BOOLEAN)""")
        )
        for dev, metric, value, ts in rows:
            # The platform's SQLAlchemy Uuid type stores 32 hex characters on SQLite.
            c.execute(
                sa.text(
                    "INSERT INTO readings (device_id, metric, value, ts) VALUES (:d, :m, :v, :t)"
                ),
                {"d": dev.hex, "m": metric, "v": value, "t": ts.strftime("%Y-%m-%d %H:%M:%S.%f")},
            )
    return f"sqlite:///{path}"


def test_extracts_exactly_one_utc_day(tmp_path):
    dev = uuid.uuid4()
    start = datetime(2026, 9, 1, tzinfo=UTC)
    url = platform_db(
        tmp_path / "p.db",
        [
            (dev, "temperature_c", 1.0, start - timedelta(microseconds=1)),  # previous day
            (dev, "temperature_c", 2.0, start),
            (dev, "temperature_c", 3.0, start + timedelta(hours=23, minutes=59)),
            (dev, "temperature_c", 4.0, start + timedelta(days=1)),  # next day
        ],
    )
    result = extract.write_day(tmp_path / "lake", DAY, url)
    table = pq.read_table(lake.partition_dir(tmp_path / "lake", lake.RAW, DAY))
    assert result["rows"] == 2
    assert sorted(table.column("value").to_pylist()) == [2.0, 3.0]
    assert table.column("device_id").to_pylist()[0] == str(dev)  # canonical dashed form
    assert lake.read_manifest(tmp_path / "lake", lake.RAW, DAY)["source"] == "platform"


def test_empty_day_still_writes_a_partition(tmp_path):
    url = platform_db(tmp_path / "p.db", [])
    assert extract.write_day(tmp_path / "lake", DAY, url)["rows"] == 0
    assert lake.exists(tmp_path / "lake", lake.RAW, DAY)


def test_platform_source_end_to_end(tmp_path, settings):
    dev = uuid.uuid4()
    start = datetime(2026, 9, 1, tzinfo=UTC)
    rows = [(dev, "temperature_c", 20.0 + i % 3, start + timedelta(minutes=i)) for i in range(1440)]
    url = platform_db(tmp_path / "p.db", rows)
    s = settings.__class__(**{**settings.__dict__, "platform_database_url": url})
    out = runner.run_day(s, DAY, source="platform")
    assert out["quality_raw"] == "pass" and out["quality_curated"] == "pass"
    assert out["transform"]["rows"] == 24
