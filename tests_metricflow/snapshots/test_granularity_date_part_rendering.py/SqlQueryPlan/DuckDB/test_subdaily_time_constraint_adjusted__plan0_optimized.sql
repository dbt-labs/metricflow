-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  metric_time__hour
  , SUM(archived_users) AS archived_users
FROM (
  -- Read Elements From Semantic Model 'users_ds_source'
  -- Metric Time Dimension 'archived_at'
  -- Constrain Time Range to [2020-01-01T00:00:02, 2020-01-01T00:00:08]
  -- Pass Only Elements: ['archived_users', 'metric_time__hour']
  SELECT
    DATE_TRUNC('hour', archived_at) AS metric_time__hour
    , 1 AS archived_users
  FROM ***************************.dim_users users_ds_source_src_28000
  WHERE DATE_TRUNC('hour', archived_at) BETWEEN '2020-01-01 00:00:02' AND '2020-01-01 00:00:08'
) subq_8
GROUP BY
  metric_time__hour
