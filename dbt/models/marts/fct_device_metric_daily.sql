-- One row per day, device and metric. Completeness compares valid readings with what the
-- device should have sent at the expected cadence.
select
    date,
    device_id,
    metric,
    sum(n_readings)                                             as readings,
    sum(n_invalid)                                              as invalid_readings,
    sum(n_duplicates)                                           as duplicate_readings,
    count(*) filter (where n_readings > 0)                      as hours_with_data,
    sum(n_readings) / (24.0 * {{ var('expected_per_hour') }})   as completeness,
    -- Weighted by readings per hour, so hours with gaps don't count as much as full ones.
    sum(mean * n_readings) / nullif(sum(n_readings), 0)         as daily_mean,
    min(min)                                                    as daily_min,
    max(max)                                                    as daily_max
from {{ ref('stg_hourly') }}
group by date, device_id, metric
