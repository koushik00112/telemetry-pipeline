import json
from datetime import date

import duckdb
import pyarrow.parquet as pq
import pytest

from pipeline import bench, cli, dbt_runner, lake, runner
from tests.conftest import DAY


@pytest.mark.dbt
def test_backfill_builds_and_tests_the_marts(settings):
    result = runner.backfill(settings, date(2026, 9, 1), date(2026, 9, 3))
    assert [d["quality_curated"] for d in result["days"]] == ["pass"] * 3
    dbt = result["dbt"]
    assert dbt["status"] == "pass", dbt
    assert dbt["models"] == {"success": 4}
    assert dbt["tests"].get("pass", 0) >= 15 and not dbt["tests"].get("fail")
    con = duckdb.connect(str(settings.warehouse), read_only=True)
    fleet = con.execute("SELECT date, devices_reporting FROM fleet_daily ORDER BY date").fetchall()
    assert [(d.isoformat(), n) for d, n in fleet] == [
        ("2026-09-01", 100),
        ("2026-09-02", 100),
        ("2026-09-03", 100),
    ]


@pytest.mark.dbt
def test_dbt_tests_fail_on_bad_curated_data(settings):
    runner.run_day(settings, DAY)
    table = pq.read_table(lake.partition_dir(settings.lake, lake.CURATED, DAY))
    doubled = table.take(list(range(table.num_rows)) + [0])  # one duplicated key
    lake.replace_partition(
        settings.lake, lake.CURATED, DAY, lambda d: pq.write_table(doubled, d / "p.parquet"), {}
    )
    result = dbt_runner.build(settings)
    assert result["status"] == "fail"
    assert any("unique_combination" in i["node"] for i in result["issues"])


def test_rerunning_a_day_is_idempotent(settings):
    result = runner.verify_idempotency(settings, DAY)
    assert result["identical"]
    assert result["first"]["raw"]["rows"] > 0 and result["first"]["curated"]["rows"] > 0


def test_rerunning_one_day_leaves_other_days_alone(settings):
    runner.run_day(settings, date(2026, 9, 1))
    runner.run_day(settings, date(2026, 9, 2))
    other = runner.snapshot(settings, date(2026, 9, 1))
    runner.run_day(settings, date(2026, 9, 2), seed=99)  # different data for day 2
    assert runner.snapshot(settings, date(2026, 9, 1)) == other
    assert runner.partition_listing(settings.lake, lake.RAW) == [
        "date=2026-09-01",
        "date=2026-09-02",
    ]


def test_days_range():
    assert runner.days(date(2026, 9, 1), date(2026, 9, 3))[-1] == date(2026, 9, 3)
    with pytest.raises(ValueError):
        runner.days(date(2026, 9, 3), date(2026, 9, 1))


def _env(monkeypatch, settings):
    monkeypatch.setenv("LAKE_ROOT", str(settings.lake))
    monkeypatch.setenv("WAREHOUSE_PATH", str(settings.warehouse))
    monkeypatch.setenv("REPORTS_ROOT", str(settings.reports))


def test_cli_exit_codes(monkeypatch, settings, capsys):
    _env(monkeypatch, settings)
    assert cli.main(["run", "--date", "2026-09-01"]) == 0
    assert json.loads(capsys.readouterr().out)["quality_curated"] == "pass"
    assert cli.main(["verify-idempotency", "--date", "2026-09-01"]) == 0
    # Corrupt the curated layer: the curated quality gate must stop the pipeline (exit 2).
    table = pq.read_table(lake.partition_dir(settings.lake, lake.CURATED, DAY))
    lake.replace_partition(
        settings.lake,
        lake.CURATED,
        DAY,
        lambda d: pq.write_table(table.slice(5), d / "p.parquet"),
        {},
    )
    assert cli.main(["quality", "--date", "2026-09-01", "--layer", "curated"]) == 2


def test_platform_source_without_a_url_is_a_clear_error(monkeypatch, settings):
    _env(monkeypatch, settings)
    monkeypatch.delenv("PLATFORM_DATABASE_URL", raising=False)
    with pytest.raises(SystemExit, match="PLATFORM_DATABASE_URL"):
        cli.main(["ingest", "--date", "2026-09-01", "--source", "platform"])


def test_bench_records_context(settings, tmp_path):
    result = bench.run(settings, [1], ["duckdb"], str(tmp_path / "bench"))
    [row] = result["rows"]
    assert row["raw_rows"] > 400_000 and row["curated_rows"] > 7000
    assert "SYNTHETIC" in result["data"]
    md = (tmp_path / "bench" / "bench.md").read_text()
    assert "not a cluster benchmark" in md
