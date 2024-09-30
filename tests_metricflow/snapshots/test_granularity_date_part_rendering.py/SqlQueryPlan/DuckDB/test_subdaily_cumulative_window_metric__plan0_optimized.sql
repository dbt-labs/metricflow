-- Join Self Over Time Range
-- Pass Only Elements: ['archived_users', 'metric_time__hour']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_9.ts AS metric_time__hour
  , SUM(subq_7.archived_users) AS subdaily_cumulative_window_metric
FROM ***************************.mf_time_spine_hour subq_9
INNER JOIN (
  -- Read Elements From Semantic Model 'users_ds_source'
  -- Metric Time Dimension 'archived_at'
  SELECT
    DATE_TRUNC('hour', archived_at) AS metric_time__hour
    , 1 AS archived_users
  FROM ***************************.dim_users users_ds_source_src_28000
) subq_7
ON
  (
    subq_7.metric_time__hour <= subq_9.ts
  ) AND (
    subq_7.metric_time__hour > subq_9.ts - INTERVAL 3 hour
  )
GROUP BY
  subq_9.ts
