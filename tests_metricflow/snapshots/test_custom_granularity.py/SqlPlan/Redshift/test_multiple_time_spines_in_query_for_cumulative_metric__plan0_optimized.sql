test_name: test_multiple_time_spines_in_query_for_cumulative_metric
test_filename: test_custom_granularity.py
sql_engine: Redshift
---
-- Join Self Over Time Range
-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['archived_users', 'metric_time__martian_day', 'metric_time__hour']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_12.martian_day AS metric_time__martian_day
  , subq_11.ts AS metric_time__hour
  , SUM(subq_9.archived_users) AS subdaily_cumulative_window_metric
FROM ***************************.mf_time_spine_hour subq_11
INNER JOIN (
  -- Read Elements From Semantic Model 'users_ds_source'
  -- Metric Time Dimension 'archived_at'
  SELECT
    DATE_TRUNC('hour', archived_at) AS metric_time__hour
    , 1 AS archived_users
  FROM ***************************.dim_users users_ds_source_src_28000
) subq_9
ON
  (
    subq_9.metric_time__hour <= subq_11.ts
  ) AND (
    subq_9.metric_time__hour > DATEADD(hour, -3, subq_11.ts)
  )
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_12
ON
  DATE_TRUNC('day', subq_11.ts) = subq_12.ds
GROUP BY
  subq_12.martian_day
  , subq_11.ts
