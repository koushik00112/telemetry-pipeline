# ADR 0003: DuckDB is the default engine; Spark is kept and must agree with it

Date: 2026-09-25
Status: Accepted (revisit if a day's data outgrows one machine)

## Context
The plan asked for PySpark aggregation. Measured on one laptop (results/bench/bench.md,
2026-09-25, Apple M3, 8 CPUs, 16 GiB): at 100× (43 M raw rows, 438 MB of Parquet) the
hourly transform takes 11.6 s with DuckDB and 56.8 s with Spark in local mode. Spark also
needed its driver memory raised from the 1 GB default to 4 GB to finish at 100×.

## Decision
- Default engine: DuckDB. Spark stays as a selectable engine (`--engine spark`).
- Both engines run the same SQL (`pipeline/transform/sql.py`), and a CI job checks that
  they produce identical aggregates (counts exact, statistics within 1e-9).

## Consequences
- A single day at 100× fits comfortably on one machine, where Spark's distribution
  overhead costs more than it saves. The benchmark doesn't show where Spark wins: that
  needs data bigger than one machine's memory, or a real cluster, neither of which was
  tested.
- Keeping Spark working and proven equivalent means switching is a flag, not a rewrite.
