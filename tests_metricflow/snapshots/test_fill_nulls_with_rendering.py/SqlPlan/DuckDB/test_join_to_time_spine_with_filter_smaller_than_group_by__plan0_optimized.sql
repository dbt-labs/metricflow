test_name: test_join_to_time_spine_with_filter_smaller_than_group_by
test_filename: test_fill_nulls_with_rendering.py
sql_engine: DuckDB
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['archived_users', 'metric_time__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , SUM(archived_users) AS archived_users_join_to_time_spine
FROM (
  -- Read Elements From Semantic Model 'users_ds_source'
  -- Metric Time Dimension 'archived_at'
  SELECT
    DATE_TRUNC('hour', archived_at) AS metric_time__hour
    , DATE_TRUNC('day', archived_at) AS metric_time__day
    , 1 AS archived_users
  FROM ***************************.dim_users users_ds_source_src_28000
) subq_7
WHERE (metric_time__hour > '2020-01-01 00:09:00') AND (metric_time__day = '2020-01-01')
GROUP BY
  metric_time__day
