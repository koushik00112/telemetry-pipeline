# Telemetry Pipeline

[![CI](https://github.com/koushik00112/telemetry-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/koushik00112/telemetry-pipeline/actions/workflows/ci.yml)

A daily batch pipeline for device telemetry. It lands raw readings as partitioned Parquet,
aggregates them hourly (DuckDB or PySpark, same SQL), checks data quality at every layer,
models the results with dbt, and runs on Airflow. Reruns and backfills are idempotent, and
there's a test that proves it.

It reads either synthetic data (built-in generator) or real readings from the
[telemetry platform](../telemetry-platform)'s Postgres database.

```
 source              lake (Parquet, one folder per UTC day)                 warehouse (DuckDB)
┌──────────┐ ingest ┌───────────────────┐ transform ┌──────────────────┐ dbt ┌──────────────────────┐
│ generator│──────▶ │ raw/readings      │─────────▶ │ curated/hourly   │───▶ │ dim_devices          │
│ or       │        │ date=YYYY-MM-DD/  │ DuckDB or │ date=YYYY-MM-DD/ │     │ fct_device_metric_daily
│ platform │        └───────────────────┘ Spark     └──────────────────┘     │ fleet_daily          │
└──────────┘            ▲ quality: raw                  ▲ quality: curated   └──────────────────────┘
                                                                              ▲ 17 dbt tests
```

[Decisions](docs/adr) · [Benchmark](results/bench/bench.md)

## Results (measured 2026-09-25, Apple M3, 16 GiB, synthetic data)

| Scale | Raw rows / day | DuckDB transform | Spark transform (local mode) |
|---|---|---|---|
| 1× | 429,576 | 0.07 s | 1.94 s |
| 10× | 4,310,041 | 0.62 s | 6.22 s |
| 100× | 43,083,528 | 11.61 s | 56.79 s |

Full table and context: [results/bench/bench.md](results/bench/bench.md). One machine, not a
cluster. Why DuckDB is the default: [ADR 0003](docs/adr/0003-duckdb-default-engine.md).

Other checks, all automated:
- **Idempotency:** rerunning a day gives identical content fingerprints in the raw and
  curated layers (`pipeline verify-idempotency`), and rerunning one day leaves the others
  untouched.
- **Data quality:** the raw checks find exactly the duplicates, NaNs and out-of-range
  values the generator injected, and the curated checks reconcile every raw reading
  (valid + invalid + duplicate = raw rows).
- **Engine parity:** Spark and DuckDB agree on every hourly row (counts exact, statistics
  within 1e-9; measured difference about 1e-14).
- **Airflow:** the DAG ran end to end under Airflow 3.3.2 (`airflow dags test`): both
  quality gates passed and all 17 dbt tests passed.

## Quick start (no Docker)

```bash
python3 -m venv .venv && .venv/bin/pip install -e ".[dev,spark]"
.venv/bin/pipeline backfill --start 2026-09-01 --end 2026-09-07   # 7 days + dbt build/test
.venv/bin/pipeline verify-idempotency --date 2026-09-03
.venv/bin/pipeline run --date 2026-09-08 --engine spark
cat reports/quality/date=2026-09-03/curated.md
.venv/bin/pytest
```

Real data from the platform:
`PLATFORM_DATABASE_URL=postgresql+psycopg://telemetry:telemetry@localhost:5432/telemetry pipeline run --date 2026-09-25 --source platform`

## Airflow (Docker)

```bash
docker compose up --build        # UI on http://localhost:8080; unpause telemetry_daily
```
The DAG (`dags/telemetry_daily.py`) runs `ingest → quality_raw → transform →
quality_curated → dbt_build` for each day, with catchup (backfill) on and one active run at
a time. A failing quality gate stops the run (exit code 2). The DAG has been run with
Airflow 3.3.2; the Compose file itself hasn't been run yet (no Docker on the build machine).

## Data quality checks

| Layer | Check | Fails the run when |
|---|---|---|
| raw | nulls, timestamps outside the partition's day | any |
| raw | duplicate rate, invalid-value rate | warn above 1% |
| raw | completeness (5th percentile of device-metrics) | warn below 80% |
| curated | unique (hour, device, metric); mean within [min, max] | any |
| curated | reconciliation with raw (distinct readings and all rows) | any mismatch |
| dbt | unique keys, not-null, relationships, completeness in [0, 1], staging-to-mart reconciliation | any; fleet invalid rate above 1% warns |

## Layout
```
pipeline/generate.py   synthetic telemetry with injected faults (deterministic)
pipeline/extract.py    land a day from the platform's database
pipeline/lake.py       partitions, atomic replacement, fingerprints
pipeline/transform/    shared SQL + DuckDB and Spark engines
pipeline/quality.py    per-layer checks and reports
pipeline/dbt_runner.py, runner.py, cli.py, bench.py
dbt/                   staging + marts + tests (dbt-duckdb)
dags/                  Airflow DAG
docker/, docker-compose.yml
```
