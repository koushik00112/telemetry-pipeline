-- Every valid reading in staging lands in the daily fact table, per day.
with s as (select date, sum(n_readings) as n from {{ ref('stg_hourly') }} group by date),
     f as (select date, sum(readings) as n from {{ ref('fct_device_metric_daily') }} group by date)
select s.date, s.n as staging, f.n as fact
from s full join f using (date)
where s.n is distinct from f.n
