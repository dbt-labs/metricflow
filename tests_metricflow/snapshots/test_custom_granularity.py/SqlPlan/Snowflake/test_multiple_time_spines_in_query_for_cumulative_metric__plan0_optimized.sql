test_name: test_multiple_time_spines_in_query_for_cumulative_metric
test_filename: test_custom_granularity.py
sql_engine: Snowflake
---
-- Join Self Over Time Range
-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['__simple_subdaily_metric_default_day', 'metric_time__alien_day', 'metric_time__hour']
-- Pass Only Elements: ['__simple_subdaily_metric_default_day', 'metric_time__alien_day', 'metric_time__hour']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  subq_15.alien_day AS metric_time__alien_day
  , subq_14.ts AS metric_time__hour
  , SUM(subq_12.__simple_subdaily_metric_default_day) AS subdaily_cumulative_window_metric
FROM ***************************.mf_time_spine_hour subq_14
INNER JOIN (
  -- Read Elements From Semantic Model 'users_ds_source'
  -- Metric Time Dimension 'archived_at'
  SELECT
    DATE_TRUNC('hour', archived_at) AS metric_time__hour
    , 1 AS __simple_subdaily_metric_default_day
  FROM ***************************.dim_users users_ds_source_src_28000
) subq_12
ON
  (
    subq_12.metric_time__hour <= subq_14.ts
  ) AND (
    subq_12.metric_time__hour > DATEADD(hour, -3, subq_14.ts)
  )
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_15
ON
  DATE_TRUNC('day', subq_14.ts) = subq_15.ds
GROUP BY
  subq_15.alien_day
  , subq_14.ts
