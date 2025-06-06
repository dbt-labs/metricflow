test_name: test_multiple_time_spines_in_query_for_cumulative_metric
test_filename: test_custom_granularity.py
sql_engine: BigQuery
---
-- Join Self Over Time Range
-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['archived_users', 'metric_time__alien_day', 'metric_time__hour']
-- Aggregate Measures
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  subq_13.alien_day AS metric_time__alien_day
  , subq_12.ts AS metric_time__hour
  , SUM(subq_10.archived_users) AS subdaily_cumulative_window_metric
FROM ***************************.mf_time_spine_hour subq_12
INNER JOIN (
  -- Read Elements From Semantic Model 'users_ds_source'
  -- Metric Time Dimension 'archived_at'
  SELECT
    DATETIME_TRUNC(archived_at, hour) AS metric_time__hour
    , 1 AS archived_users
  FROM ***************************.dim_users users_ds_source_src_28000
) subq_10
ON
  (
    subq_10.metric_time__hour <= subq_12.ts
  ) AND (
    subq_10.metric_time__hour > DATE_SUB(CAST(subq_12.ts AS DATETIME), INTERVAL 3 hour)
  )
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_13
ON
  DATETIME_TRUNC(subq_12.ts, day) = subq_13.ds
GROUP BY
  metric_time__alien_day
  , metric_time__hour
