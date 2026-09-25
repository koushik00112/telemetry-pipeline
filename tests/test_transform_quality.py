import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pipeline import generate, lake, quality, transform
from tests.conftest import DAY

HOUR = 1_788_220_800_000_000  # 2026-09-01T00:00:00Z in microseconds


def write_raw(root, rows):
    table = pa.table(
        {
            "device_id": [r[0] for r in rows],
            "metric": [r[1] for r in rows],
            "value": pa.array([r[2] for r in rows], pa.float64()),
            "ts": pa.array([HOUR + r[3] * 60_000_000 for r in rows], pa.timestamp("us", tz="UTC")),
        }
    )
    lake.replace_partition(
        root, lake.RAW, DAY, lambda d: pq.write_table(table, d / "p.parquet"), {}
    )


def curated(root):
    return pq.read_table(lake.partition_dir(root, lake.CURATED, DAY)).to_pylist()


def test_known_answer_dedupe_invalid_and_stats(tmp_path):
    write_raw(
        tmp_path,
        [
            ("d1", "temperature_c", 20.0, 0),
            ("d1", "temperature_c", 20.0, 0),  # exact duplicate
            ("d1", "temperature_c", 22.0, 1),
            ("d1", "temperature_c", 21.0, 2),
            ("d1", "temperature_c", 25.0, 2),  # conflicting resend: the larger value wins
            ("d1", "temperature_c", 999.0, 3),  # out of range
            ("d1", "temperature_c", float("nan"), 4),
            ("d1", "humidity_pct", 50.0, 0),
        ],
    )
    transform.run("duckdb", tmp_path, DAY)
    rows = {r["metric"]: r for r in curated(tmp_path)}
    t = rows["temperature_c"]
    assert (t["n_readings"], t["n_invalid"], t["n_duplicates"]) == (3, 2, 2)
    assert t["min"] == 20.0 and t["max"] == 25.0
    assert t["mean"] == pytest.approx((20 + 22 + 25) / 3)
    # Valid values 20, 22, 25: linear interpolation at position 0.95 * (3 - 1) = 1.9.
    assert t["p95"] == pytest.approx(22 + 0.9 * (25 - 22))
    assert rows["humidity_pct"]["stddev"] is None  # one reading: sample stddev undefined


def test_quality_checks_find_exactly_what_was_injected(tmp_path):
    inj = generate.write_day(tmp_path, DAY)
    raw = {c["name"]: c for c in quality.run(tmp_path, tmp_path / "r", "raw", DAY)["checks"]}
    assert raw["duplicate_rate"]["detail"] == f"{inj.duplicates} duplicate rows"
    assert raw["invalid_value_rate"]["detail"] == (
        f"{inj.out_of_range + inj.nans} invalid ({inj.nans} NaN, {inj.out_of_range} out of range)"
    )
    transform.run("duckdb", tmp_path, DAY)
    report = quality.run(tmp_path, tmp_path / "r", "curated", DAY)
    assert report["status"] == "pass"
    assert (tmp_path / "r" / "quality" / "date=2026-09-01" / "curated.md").exists()


def test_reconciliation_catches_rows_lost_in_the_transform(tmp_path):
    generate.write_day(tmp_path, DAY)
    transform.run("duckdb", tmp_path, DAY)
    table = pq.read_table(lake.partition_dir(tmp_path, lake.CURATED, DAY))
    lake.replace_partition(
        tmp_path, lake.CURATED, DAY, lambda d: pq.write_table(table.slice(10), d / "p.parquet"), {}
    )
    checks = {
        c["name"]: c["status"]
        for c in quality.run(tmp_path, tmp_path / "r", "curated", DAY)["checks"]
    }
    assert checks["reconcile_distinct_readings"] == "fail"
    assert checks["reconcile_all_rows"] == "fail"


def test_raw_rows_filed_under_the_wrong_day_fail(tmp_path):
    write_raw(tmp_path, [("d1", "temperature_c", 20.0, 0), ("d1", "temperature_c", 20.0, 60 * 25)])
    report = quality.run(tmp_path, tmp_path / "r", "raw", DAY)
    assert report["status"] == "fail"


def test_transform_needs_raw_data(tmp_path):
    with pytest.raises(FileNotFoundError):
        transform.run("duckdb", tmp_path, DAY)
    with pytest.raises(ValueError):
        transform.run("pandas", tmp_path, DAY)


@pytest.mark.spark
def test_spark_and_duckdb_agree(tmp_path):
    pytest.importorskip("pyspark")
    generate.write_day(tmp_path, DAY)
    transform.run("duckdb", tmp_path, DAY)
    duck = pq.read_table(lake.partition_dir(tmp_path, lake.CURATED, DAY))
    transform.run("spark", tmp_path, DAY)
    con = duckdb.connect()
    con.register("duck", duck)
    diff = con.execute(f"""
        SELECT count(*), count(a.hour), count(b.hour),
               max(abs(a.mean - b.mean)), max(abs(a.p95 - b.p95)), max(abs(a.stddev - b.stddev)),
               count(*) FILTER (WHERE a.n_readings <> b.n_readings OR a.n_invalid <> b.n_invalid
                                OR a.n_duplicates <> b.n_duplicates)
        FROM duck a FULL JOIN {lake.read_sql(tmp_path, lake.CURATED, DAY)} b
        USING (hour, device_id, metric)""").fetchone()
    total, in_a, in_b, dmean, dp95, dstd, count_mismatch = diff
    assert total == in_a == in_b > 0
    assert count_mismatch == 0
    assert max(dmean, dp95, dstd) < 1e-9
    assert quality.run(tmp_path, tmp_path / "r", "curated", DAY)["status"] == "pass"
