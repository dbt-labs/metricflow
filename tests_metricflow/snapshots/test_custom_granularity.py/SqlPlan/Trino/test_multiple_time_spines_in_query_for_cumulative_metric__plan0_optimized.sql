test_name: test_multiple_time_spines_in_query_for_cumulative_metric
test_filename: test_custom_granularity.py
sql_engine: Trino
---
-- Join Self Over Time Range
-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['simple_subdaily_metric_default_day', 'metric_time__alien_day', 'metric_time__hour']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  subq_14.alien_day AS metric_time__alien_day
  , subq_13.ts AS metric_time__hour
  , SUM(subq_11.simple_subdaily_metric_default_day) AS subdaily_cumulative_window_metric
FROM ***************************.mf_time_spine_hour subq_13
INNER JOIN (
  -- Read Elements From Semantic Model 'users_ds_source'
  -- Metric Time Dimension 'archived_at'
  SELECT
    DATE_TRUNC('hour', archived_at) AS metric_time__hour
    , 1 AS simple_subdaily_metric_default_day
  FROM ***************************.dim_users users_ds_source_src_28000
) subq_11
ON
  (
    subq_11.metric_time__hour <= subq_13.ts
  ) AND (
    subq_11.metric_time__hour > DATE_ADD('hour', -3, subq_13.ts)
  )
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_14
ON
  DATE_TRUNC('day', subq_13.ts) = subq_14.ds
GROUP BY
  subq_14.alien_day
  , subq_13.ts
