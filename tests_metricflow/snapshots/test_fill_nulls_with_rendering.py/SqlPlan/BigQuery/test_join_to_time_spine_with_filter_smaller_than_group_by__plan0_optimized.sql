test_name: test_join_to_time_spine_with_filter_smaller_than_group_by
test_filename: test_fill_nulls_with_rendering.py
sql_engine: BigQuery
---
-- Join to Time Spine Dataset
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  subq_19.metric_time__day AS metric_time__day
  , subq_15.archived_users AS archived_users_join_to_time_spine
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['metric_time__day']
  SELECT
    metric_time__day
  FROM (
    -- Read From Time Spine 'mf_time_spine_hour'
    -- Change Column Aliases
    SELECT
      ts AS metric_time__hour
      , DATETIME_TRUNC(ts, day) AS metric_time__day
    FROM ***************************.mf_time_spine_hour time_spine_src_28005
  ) subq_17
  WHERE (metric_time__hour > '2020-01-01 00:09:00') AND (metric_time__day = '2020-01-01')
  GROUP BY
    metric_time__day
) subq_19
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
  ) subq_12
  WHERE (metric_time__hour > '2020-01-01 00:09:00') AND (metric_time__day = '2020-01-01')
  GROUP BY
    metric_time__day
) subq_15
ON
  subq_19.metric_time__day = subq_15.metric_time__day
