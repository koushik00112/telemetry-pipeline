"""The hourly aggregation, written once for both engines.

Rules:
  1. Deduplicate on (device_id, metric, ts). If a resend carries a different value, keep
     the larger one, so the result doesn't depend on row order.
  2. A reading is invalid if it's NaN or outside the metric's physical range. Invalid
     readings are counted but excluded from the statistics.
  3. Aggregate per (hour, device_id, metric).

Only the percentile function differs between dialects. Both are exact percentiles with
linear interpolation, so results agree.
"""

from pipeline.config import METRIC_RANGES

PERCENTILE = {"duckdb": "quantile_cont(value, 0.95)", "spark": "percentile(value, 0.95)"}


def _invalid_expr() -> str:
    cases = " ".join(
        f"WHEN metric = '{m}' THEN (value < {lo} OR value > {hi})"
        for m, (lo, hi) in sorted(METRIC_RANGES.items())
    )
    return f"(isnan(value) OR CASE {cases} ELSE false END)"


def hourly(dialect: str, source: str = "raw") -> str:
    p95 = PERCENTILE[dialect].replace("value", "CASE WHEN invalid = 0 THEN value END")
    valid = "CASE WHEN invalid = 0 THEN value END"
    return f"""
WITH deduped AS (
    SELECT device_id, metric, ts, max(value) AS value, count(*) - 1 AS dup_count
    FROM {source}
    GROUP BY device_id, metric, ts
),
flagged AS (
    SELECT *, CASE WHEN {_invalid_expr()} THEN 1 ELSE 0 END AS invalid
    FROM deduped
)
SELECT
    date_trunc('hour', ts)            AS hour,
    device_id,
    metric,
    CAST(sum(1 - invalid) AS BIGINT)  AS n_readings,
    CAST(sum(invalid) AS BIGINT)      AS n_invalid,
    CAST(sum(dup_count) AS BIGINT)    AS n_duplicates,
    avg({valid})                      AS mean,
    min({valid})                      AS min,
    max({valid})                      AS max,
    stddev_samp({valid})              AS stddev,
    {p95}                             AS p95
FROM flagged
GROUP BY date_trunc('hour', ts), device_id, metric
ORDER BY hour, device_id, metric
"""
