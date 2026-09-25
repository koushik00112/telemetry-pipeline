from datetime import date

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pipeline import generate, lake
from tests.conftest import DAY


def test_generation_is_deterministic(tmp_path):
    a = generate.write_day(tmp_path / "a", DAY, seed=3)
    b = generate.write_day(tmp_path / "b", DAY, seed=3)
    generate.write_day(tmp_path / "c", DAY, seed=4)
    fa, fb, fc = (lake.fingerprint(tmp_path / x, lake.RAW, DAY) for x in "abc")
    assert a == b and fa == fb
    assert fa != fc


def test_generated_day_shape_and_injected_counts(tmp_path):
    inj = generate.write_day(tmp_path, DAY, scale=1)
    table = pq.read_table(lake.partition_dir(tmp_path, lake.RAW, DAY))
    assert table.column_names == lake.RAW_SCHEMA.names
    assert table.num_rows == inj.rows_written == inj.distinct_keys + inj.duplicates
    assert inj.devices == 100 and inj.duplicates > 0 and inj.out_of_range > 0 and inj.nans > 0
    # Everything is inside the UTC day.
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    lo, hi = con.execute(
        f"SELECT min(ts)::DATE, max(ts)::DATE FROM {lake.read_sql(tmp_path, lake.RAW, DAY)}"
    ).fetchone()
    assert lo == hi == DAY
    assert lake.read_manifest(tmp_path, lake.RAW, DAY)["note"].startswith("SYNTHETIC")


def test_scale_multiplies_devices(tmp_path):
    inj = generate.write_day(tmp_path, DAY, scale=3)
    assert inj.devices == 300


def test_failed_write_leaves_the_old_partition_untouched(tmp_path):
    generate.write_day(tmp_path, DAY)
    before = lake.fingerprint(tmp_path, lake.RAW, DAY)

    def broken(tmp):
        pq.write_table(pa.table({"x": [1]}), tmp / "part-00000.parquet")
        raise RuntimeError("disk full")

    with pytest.raises(RuntimeError):
        lake.replace_partition(tmp_path, lake.RAW, DAY, broken, {})
    assert lake.fingerprint(tmp_path, lake.RAW, DAY) == before
    assert not list((tmp_path / lake.RAW).glob(".tmp-*"))


def test_fingerprint_ignores_row_order_and_file_split(tmp_path):
    rows = pa.table(
        {
            "device_id": ["a", "b", "c"],
            "metric": ["m"] * 3,
            "value": [1.0, 2.0, 3.0],
            "ts": pa.array([1, 2, 3], pa.timestamp("us", tz="UTC")),
        }
    )
    lake.replace_partition(
        tmp_path / "x", lake.RAW, DAY, lambda d: pq.write_table(rows, d / "p.parquet"), {}
    )

    def split_and_shuffled(d):
        pq.write_table(rows.take([2]), d / "p1.parquet")
        pq.write_table(rows.take([1, 0]), d / "p2.parquet")

    lake.replace_partition(tmp_path / "y", lake.RAW, DAY, split_and_shuffled, {})
    assert lake.fingerprint(tmp_path / "x", lake.RAW, DAY) == lake.fingerprint(
        tmp_path / "y", lake.RAW, DAY
    )


def test_sql_str_escapes_quotes():
    assert lake.sql_str("/tmp/o'brien/*.parquet") == "'/tmp/o''brien/*.parquet'"


def test_partitions_are_per_day(tmp_path):
    generate.write_day(tmp_path, date(2026, 9, 1))
    generate.write_day(tmp_path, date(2026, 9, 2))
    assert sorted(p.name for p in (tmp_path / lake.RAW).iterdir()) == [
        "date=2026-09-01",
        "date=2026-09-02",
    ]
