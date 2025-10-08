test_name: test_multiple_time_spines_in_query_for_join_to_time_spine_metric
test_filename: test_custom_granularity.py
sql_engine: Postgres
---
-- Join to Time Spine Dataset
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  subq_19.metric_time__alien_day AS metric_time__alien_day
  , subq_19.metric_time__hour AS metric_time__hour
  , subq_15.subdaily_join_to_time_spine_metric AS subdaily_join_to_time_spine_metric
FROM (
  -- Change Column Aliases
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['metric_time__alien_day', 'metric_time__hour']
  SELECT
    subq_17.alien_day AS metric_time__alien_day
    , time_spine_src_28005.ts AS metric_time__hour
  FROM ***************************.mf_time_spine_hour time_spine_src_28005
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_17
  ON
    DATE_TRUNC('day', time_spine_src_28005.ts) = subq_17.ds
) subq_19
LEFT OUTER JOIN (
  -- Metric Time Dimension 'archived_at'
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['subdaily_join_to_time_spine_metric', 'metric_time__alien_day', 'metric_time__hour']
  -- Aggregate Inputs for Simple Metrics
  SELECT
    subq_12.alien_day AS metric_time__alien_day
    , subq_11.archived_at__hour AS metric_time__hour
    , SUM(subq_11.subdaily_join_to_time_spine_metric) AS subdaily_join_to_time_spine_metric
  FROM (
    -- Read Elements From Semantic Model 'users_ds_source'
    SELECT
      1 AS subdaily_join_to_time_spine_metric
      , DATE_TRUNC('hour', archived_at) AS archived_at__hour
      , DATE_TRUNC('day', archived_at) AS archived_at__day
    FROM ***************************.dim_users users_ds_source_src_28000
  ) subq_11
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_12
  ON
    subq_11.archived_at__day = subq_12.ds
  GROUP BY
    subq_12.alien_day
    , subq_11.archived_at__hour
) subq_15
ON
  subq_19.metric_time__hour = subq_15.metric_time__hour
