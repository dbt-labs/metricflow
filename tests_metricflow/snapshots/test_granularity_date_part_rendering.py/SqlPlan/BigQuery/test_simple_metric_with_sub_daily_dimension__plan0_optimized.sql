test_name: test_simple_metric_with_sub_daily_dimension
test_filename: test_granularity_date_part_rendering.py
sql_engine: BigQuery
---
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  user__archived_at__hour
  , SUM(new_users) AS new_users
FROM (
  -- Read Elements From Semantic Model 'users_ds_source'
  -- Metric Time Dimension 'created_at'
  -- Pass Only Elements: ['new_users', 'user__archived_at__hour']
  SELECT
    DATETIME_TRUNC(archived_at, hour) AS user__archived_at__hour
    , 1 AS new_users
  FROM ***************************.dim_users users_ds_source_src_28000
) nr_subq_4
GROUP BY
  user__archived_at__hour
