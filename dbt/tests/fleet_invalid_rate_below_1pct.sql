{{ config(severity = 'warn') }}
-- More than 1% invalid readings in a day usually means a sensor or firmware problem.
select * from {{ ref('fleet_daily') }} where invalid_rate > 0.01
