test_name: test_multiple_time_spines_in_query_for_join_to_time_spine_metric
test_filename: test_custom_granularity.py
sql_engine: Databricks
---
-- Join to Time Spine Dataset
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  subq_23.metric_time__alien_day AS metric_time__alien_day
  , subq_23.metric_time__hour AS metric_time__hour
  , subq_18.__subdaily_join_to_time_spine_metric AS subdaily_join_to_time_spine_metric
FROM (
  -- Change Column Aliases
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['metric_time__alien_day', 'metric_time__hour']
  -- Pass Only Elements: ['metric_time__alien_day', 'metric_time__hour']
  SELECT
    subq_20.alien_day AS metric_time__alien_day
    , time_spine_src_28005.ts AS metric_time__hour
  FROM ***************************.mf_time_spine_hour time_spine_src_28005
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_20
  ON
    DATE_TRUNC('day', time_spine_src_28005.ts) = subq_20.ds
) subq_23
LEFT OUTER JOIN (
  -- Metric Time Dimension 'archived_at'
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['__subdaily_join_to_time_spine_metric', 'metric_time__alien_day', 'metric_time__hour']
  -- Pass Only Elements: ['__subdaily_join_to_time_spine_metric', 'metric_time__alien_day', 'metric_time__hour']
  -- Aggregate Inputs for Simple Metrics
  SELECT
    subq_14.alien_day AS metric_time__alien_day
    , subq_13.archived_at__hour AS metric_time__hour
    , SUM(subq_13.__subdaily_join_to_time_spine_metric) AS __subdaily_join_to_time_spine_metric
  FROM (
    -- Read Elements From Semantic Model 'users_ds_source'
    SELECT
      1 AS __subdaily_join_to_time_spine_metric
      , DATE_TRUNC('hour', archived_at) AS archived_at__hour
      , DATE_TRUNC('day', archived_at) AS archived_at__day
    FROM ***************************.dim_users users_ds_source_src_28000
  ) subq_13
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_14
  ON
    subq_13.archived_at__day = subq_14.ds
  GROUP BY
    subq_14.alien_day
    , subq_13.archived_at__hour
) subq_18
ON
  subq_23.metric_time__hour = subq_18.metric_time__hour
