test_name: test_join_to_time_spine_with_filter_smaller_than_group_by
test_filename: test_fill_nulls_with_rendering.py
sql_engine: Redshift
---
-- Join to Time Spine Dataset
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  subq_23.metric_time__day AS metric_time__day
  , subq_18.__archived_users_join_to_time_spine AS archived_users_join_to_time_spine
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['metric_time__day']
  SELECT
    metric_time__day
  FROM (
    -- Read From Time Spine 'mf_time_spine_hour'
    -- Change Column Aliases
    -- Pass Only Elements: ['metric_time__day', 'metric_time__hour']
    SELECT
      ts AS metric_time__hour
      , DATE_TRUNC('day', ts) AS metric_time__day
    FROM ***************************.mf_time_spine_hour time_spine_src_28005
  ) subq_21
  WHERE (metric_time__hour > '2020-01-01 00:09:00') AND (metric_time__day = '2020-01-01')
  GROUP BY
    metric_time__day
) subq_23
LEFT OUTER JOIN (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['__archived_users_join_to_time_spine', 'metric_time__day']
  -- Aggregate Inputs for Simple Metrics
  SELECT
    metric_time__day
    , SUM(archived_users_join_to_time_spine) AS __archived_users_join_to_time_spine
  FROM (
    -- Read Elements From Semantic Model 'users_ds_source'
    -- Metric Time Dimension 'archived_at'
    -- Pass Only Elements: ['__archived_users_join_to_time_spine', 'metric_time__day', 'metric_time__hour']
    SELECT
      DATE_TRUNC('hour', archived_at) AS metric_time__hour
      , DATE_TRUNC('day', archived_at) AS metric_time__day
      , 1 AS archived_users_join_to_time_spine
    FROM ***************************.dim_users users_ds_source_src_28000
  ) subq_15
  WHERE (metric_time__hour > '2020-01-01 00:09:00') AND (metric_time__day = '2020-01-01')
  GROUP BY
    metric_time__day
) subq_18
ON
  subq_23.metric_time__day = subq_18.metric_time__day
