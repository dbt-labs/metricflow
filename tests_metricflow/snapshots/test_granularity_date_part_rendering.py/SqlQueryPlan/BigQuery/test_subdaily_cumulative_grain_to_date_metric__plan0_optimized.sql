test_name: test_subdaily_cumulative_grain_to_date_metric
test_filename: test_granularity_date_part_rendering.py
sql_engine: BigQuery
---
-- Join Self Over Time Range
-- Pass Only Elements: ['archived_users', 'metric_time__hour']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_10.ts AS metric_time__hour
  , SUM(subq_8.archived_users) AS subdaily_cumulative_grain_to_date_metric
FROM ***************************.mf_time_spine_hour subq_10
INNER JOIN (
  -- Read Elements From Semantic Model 'users_ds_source'
  -- Metric Time Dimension 'archived_at'
  SELECT
    DATETIME_TRUNC(archived_at, hour) AS metric_time__hour
    , 1 AS archived_users
  FROM ***************************.dim_users users_ds_source_src_28000
) subq_8
ON
  (
    subq_8.metric_time__hour <= subq_10.ts
  ) AND (
    subq_8.metric_time__hour >= DATETIME_TRUNC(subq_10.ts, hour)
  )
GROUP BY
  metric_time__hour
