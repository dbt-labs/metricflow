-- Compute Metrics via Expressions
SELECT
  metric_time__hour
  , archived_users AS subdaily_offset_grain_to_date_metric
FROM (
  -- Join to Time Spine Dataset
  -- Pass Only Elements: ['archived_users', 'metric_time__hour']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_10.ts AS metric_time__hour
    , SUM(subq_8.archived_users) AS archived_users
  FROM ***************************.mf_time_spine_hour subq_10
  INNER JOIN (
    -- Read Elements From Semantic Model 'users_ds_source'
    -- Metric Time Dimension 'archived_at'
    SELECT
      DATE_TRUNC('hour', archived_at) AS metric_time__hour
      , 1 AS archived_users
    FROM ***************************.dim_users users_ds_source_src_28000
  ) subq_8
  ON
    DATE_TRUNC('hour', subq_10.ts) = subq_8.metric_time__hour
  GROUP BY
    subq_10.ts
) subq_13
