test_name: test_subdaily_offset_window_metric
test_filename: test_granularity_date_part_rendering.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
SELECT
  metric_time__hour
  , archived_users AS subdaily_offset_window_metric
FROM (
  -- Join to Time Spine Dataset
  -- Pass Only Elements: ['archived_users', 'metric_time__hour']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    time_spine_src_28005.ts AS metric_time__hour
    , SUM(subq_10.archived_users) AS archived_users
  FROM ***************************.mf_time_spine_hour time_spine_src_28005
  INNER JOIN (
    -- Read Elements From Semantic Model 'users_ds_source'
    -- Metric Time Dimension 'archived_at'
    SELECT
      DATE_TRUNC('hour', archived_at) AS metric_time__hour
      , 1 AS archived_users
    FROM ***************************.dim_users users_ds_source_src_28000
  ) subq_10
  ON
    time_spine_src_28005.ts - INTERVAL 1 hour = subq_10.metric_time__hour
  GROUP BY
    time_spine_src_28005.ts
) subq_17
