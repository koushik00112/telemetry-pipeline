select
    device_id,
    min(hour)                         as first_seen,
    max(hour)                         as last_seen,
    count(distinct date)              as days_reporting,
    count(distinct metric)            as metrics_reported
from {{ ref('stg_hourly') }}
where n_readings > 0
group by device_id
