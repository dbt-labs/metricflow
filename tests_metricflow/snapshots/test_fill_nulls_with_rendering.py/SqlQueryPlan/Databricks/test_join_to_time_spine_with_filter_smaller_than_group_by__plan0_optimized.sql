-- Join to Time Spine Dataset
-- Compute Metrics via Expressions
SELECT
  subq_15.metric_time__day AS metric_time__day
  , subq_14.archived_users AS archived_users_join_to_time_spine
FROM (
  -- Time Spine
  SELECT
    ts AS metric_time__hour
    , DATE_TRUNC('day', ts) AS metric_time__day
    , DATE_TRUNC('year', ts) AS metric_time__year
  FROM ***************************.mf_time_spine_hour subq_16
  WHERE (
    metric_time__hour > '2020-01-01 00:00:00'
  ) AND (
    metric_time__year < '2021-01-01'
  )
) subq_15
LEFT OUTER JOIN (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['archived_users', 'metric_time__day']
  -- Aggregate Measures
  SELECT
    metric_time__day
    , SUM(archived_users) AS archived_users
  FROM (
    -- Read Elements From Semantic Model 'users_ds_source'
    -- Metric Time Dimension 'archived_at'
    -- Pass Only Elements: ['archived_users', 'metric_time__day', 'metric_time__hour', 'metric_time__year']
    SELECT
      DATE_TRUNC('hour', archived_at) AS metric_time__hour
      , DATE_TRUNC('day', archived_at) AS metric_time__day
      , DATE_TRUNC('year', archived_at) AS metric_time__year
      , 1 AS archived_users
    FROM ***************************.dim_users users_ds_source_src_28000
  ) subq_11
  WHERE (metric_time__hour > '2020-01-01 00:00:00') AND (metric_time__year < '2021-01-01')
  GROUP BY
    metric_time__day
) subq_14
ON
  subq_15.metric_time__day = subq_14.metric_time__day
