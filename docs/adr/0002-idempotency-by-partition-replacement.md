# ADR 0002: Idempotency by replacing whole daily partitions; dbt marts rebuilt in full

Date: 2026-09-25
Status: Accepted

## Context
A rerun of any day (after a failure, a bug fix or late data) must give the same result as
the first run, and must never leave a half-written day behind.

## Decision
- Each step writes one day's partition into a temporary directory, then swaps it in with a
  directory rename (`lake.replace_partition`). Readers see the old day or the new day,
  never a mix.
- Proof: `pipeline verify-idempotency` runs a day twice and compares order-independent
  content fingerprints of the raw and curated layers. Floats are rounded to 9 decimals,
  because parallel sums can differ in the last bits between runs.
- The dbt marts are `table` models rebuilt from all curated partitions on every run.

## Consequences
- Simple, and trivially correct for backfills: rerun any range, then rebuild the marts.
- A full rebuild costs time proportional to history. At about 7,200 curated rows per day at
  1×, a year is about 2.6 M rows, which DuckDB rebuilds in seconds. Move the marts to
  incremental models (`insert_overwrite` by date) once rebuild time matters, and keep the
  same partition-replacement semantics.
- Late data for a past day needs that day to be rerun. Nothing does that automatically yet.
