test_name: test_simple_metric_with_sub_daily_dimension
test_filename: test_granularity_date_part_rendering.py
sql_engine: Snowflake
---
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  user__archived_at__hour
  , SUM(__new_users) AS new_users
FROM (
  -- Read Elements From Semantic Model 'users_ds_source'
  -- Metric Time Dimension 'created_at'
  -- Pass Only Elements: ['__new_users', 'user__archived_at__hour']
  -- Pass Only Elements: ['__new_users', 'user__archived_at__hour']
  SELECT
    DATE_TRUNC('hour', archived_at) AS user__archived_at__hour
    , 1 AS __new_users
  FROM ***************************.dim_users users_ds_source_src_28000
) subq_9
GROUP BY
  user__archived_at__hour
