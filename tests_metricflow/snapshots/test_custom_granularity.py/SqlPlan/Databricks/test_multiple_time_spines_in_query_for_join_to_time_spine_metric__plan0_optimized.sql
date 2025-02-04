test_name: test_multiple_time_spines_in_query_for_join_to_time_spine_metric
test_filename: test_custom_granularity.py
sql_engine: Databricks
---
-- Join to Time Spine Dataset
-- Compute Metrics via Expressions
SELECT
  subq_18.metric_time__martian_day AS metric_time__martian_day
  , subq_18.metric_time__hour AS metric_time__hour
  , subq_14.archived_users AS subdaily_join_to_time_spine_metric
FROM (
  -- Change Column Aliases
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['metric_time__martian_day', 'metric_time__hour']
  SELECT
    subq_16.martian_day AS metric_time__martian_day
    , time_spine_src_28005.ts AS metric_time__hour
  FROM ***************************.mf_time_spine_hour time_spine_src_28005
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_16
  ON
    DATE_TRUNC('day', time_spine_src_28005.ts) = subq_16.ds
) subq_18
LEFT OUTER JOIN (
  -- Metric Time Dimension 'archived_at'
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['archived_users', 'metric_time__martian_day', 'metric_time__hour']
  -- Aggregate Measures
  SELECT
    subq_11.martian_day AS metric_time__martian_day
    , subq_10.archived_at__hour AS metric_time__hour
    , SUM(subq_10.archived_users) AS archived_users
  FROM (
    -- Read Elements From Semantic Model 'users_ds_source'
    SELECT
      1 AS archived_users
      , DATE_TRUNC('hour', archived_at) AS archived_at__hour
      , DATE_TRUNC('day', archived_at) AS archived_at__day
    FROM ***************************.dim_users users_ds_source_src_28000
  ) subq_10
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_11
  ON
    subq_10.archived_at__day = subq_11.ds
  GROUP BY
    subq_11.martian_day
    , subq_10.archived_at__hour
) subq_14
ON
  subq_18.metric_time__hour = subq_14.metric_time__hour
