-- Reads every curated partition. `date` comes from the Hive partition folder name.
select
    cast(date as date)        as date,
    hour,
    device_id,
    metric,
    n_readings,
    n_invalid,
    n_duplicates,
    mean,
    min,
    max,
    stddev,
    p95
from read_parquet('{{ var("lake") }}/curated/hourly/date=*/*.parquet', hive_partitioning = true)
