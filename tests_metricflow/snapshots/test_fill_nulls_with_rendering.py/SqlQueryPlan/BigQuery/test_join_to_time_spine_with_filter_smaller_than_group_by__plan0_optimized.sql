test_name: test_join_to_time_spine_with_filter_smaller_than_group_by
test_filename: test_fill_nulls_with_rendering.py
sql_engine: BigQuery
---
-- Join to Time Spine Dataset
-- Compute Metrics via Expressions
SELECT
  subq_14.metric_time__day AS metric_time__day
  , subq_13.archived_users AS archived_users_join_to_time_spine
FROM (
  -- Filter Time Spine
  SELECT
    metric_time__day
  FROM (
    -- Time Spine
    SELECT
      DATETIME_TRUNC(ts, day) AS metric_time__day
      , ts AS metric_time__hour
    FROM ***************************.mf_time_spine_hour subq_15
  ) subq_16
  WHERE (
    metric_time__hour > '2020-01-01 00:09:00'
  ) AND (
    metric_time__day = '2020-01-01'
  )
  GROUP BY
    metric_time__day
) subq_14
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
    SELECT
      DATETIME_TRUNC(archived_at, hour) AS metric_time__hour
      , DATETIME_TRUNC(archived_at, day) AS metric_time__day
      , 1 AS archived_users
    FROM ***************************.dim_users users_ds_source_src_28000
  ) subq_10
  WHERE (metric_time__hour > '2020-01-01 00:09:00') AND (metric_time__day = '2020-01-01')
  GROUP BY
    metric_time__day
) subq_13
ON
  subq_14.metric_time__day = subq_13.metric_time__day
