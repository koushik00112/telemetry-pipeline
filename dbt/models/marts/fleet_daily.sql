-- Fleet health per day: the table a dashboard or daily report would read.
with per_device as (
    select date, device_id, avg(completeness) as completeness,
           sum(invalid_readings) as invalid, sum(duplicate_readings) as duplicates,
           sum(readings) as readings
    from {{ ref('fct_device_metric_daily') }}
    group by date, device_id
)
select
    date,
    count(*)                                                          as devices_reporting,
    avg(completeness)                                                 as avg_completeness,
    count(*) filter (where completeness < 0.9)                        as devices_below_90pct,
    sum(invalid) / nullif(sum(readings + invalid), 0)                 as invalid_rate,
    sum(duplicates) / nullif(sum(readings + invalid + duplicates), 0) as duplicate_rate,
    sum(readings)                                                     as valid_readings
from per_device
group by date
