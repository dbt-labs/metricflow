-- Join Self Over Time Range
-- Pass Only Elements: ['archived_users', 'metric_time__hour']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_10.ts AS metric_time__hour
  , SUM(subq_8.archived_users) AS subdaily_cumulative_window_metric
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
    subq_8.metric_time__hour > DATE_SUB(CAST(subq_10.ts AS DATETIME), INTERVAL 3 hour)
  )
GROUP BY
  metric_time__hour
