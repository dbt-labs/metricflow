test_name: test_subdaily_cumulative_grain_to_date_metric
test_filename: test_granularity_date_part_rendering.py
sql_engine: Trino
---
-- Join Self Over Time Range
-- Pass Only Elements: ['simple_subdaily_metric_default_day', 'metric_time__hour']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  subq_12.ts AS metric_time__hour
  , SUM(subq_10.simple_subdaily_metric_default_day) AS subdaily_cumulative_grain_to_date_metric
FROM ***************************.mf_time_spine_hour subq_12
INNER JOIN (
  -- Read Elements From Semantic Model 'users_ds_source'
  -- Metric Time Dimension 'archived_at'
  SELECT
    DATE_TRUNC('hour', archived_at) AS metric_time__hour
    , 1 AS simple_subdaily_metric_default_day
  FROM ***************************.dim_users users_ds_source_src_28000
) subq_10
ON
  (
    subq_10.metric_time__hour <= subq_12.ts
  ) AND (
    subq_10.metric_time__hour >= DATE_TRUNC('hour', subq_12.ts)
  )
GROUP BY
  subq_12.ts
