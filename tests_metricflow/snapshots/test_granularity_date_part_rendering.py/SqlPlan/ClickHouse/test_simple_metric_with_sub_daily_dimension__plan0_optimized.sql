test_name: test_simple_metric_with_sub_daily_dimension
test_filename: test_granularity_date_part_rendering.py
sql_engine: ClickHouse
---
SELECT
  user__archived_at__hour
  , SUM(__new_users) AS new_users
FROM (
  SELECT
    toStartOfHour(archived_at) AS user__archived_at__hour
    , 1 AS __new_users
  FROM ***************************.dim_users users_ds_source_src_28000
) subq_9
GROUP BY
  user__archived_at__hour
