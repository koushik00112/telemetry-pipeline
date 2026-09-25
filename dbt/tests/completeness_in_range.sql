-- A device can't send more than 100% of its expected readings once duplicates are removed.
select * from {{ ref('fct_device_metric_daily') }}
where completeness < 0 or completeness > 1.0
