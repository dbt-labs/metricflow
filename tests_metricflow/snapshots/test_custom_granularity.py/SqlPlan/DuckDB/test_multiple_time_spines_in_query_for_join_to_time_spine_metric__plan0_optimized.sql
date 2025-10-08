test_name: test_multiple_time_spines_in_query_for_join_to_time_spine_metric
test_filename: test_custom_granularity.py
sql_engine: DuckDB
---
-- Metric Time Dimension 'archived_at'
-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['archived_users', 'metric_time__alien_day', 'metric_time__hour']
-- Aggregate Measures
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  subq_7.alien_day AS metric_time__alien_day
  , subq_6.archived_at__hour AS metric_time__hour
  , SUM(subq_6.archived_users) AS subdaily_join_to_time_spine_metric
FROM (
  -- Read Elements From Semantic Model 'users_ds_source'
  SELECT
    1 AS archived_users
    , DATE_TRUNC('hour', archived_at) AS archived_at__hour
    , DATE_TRUNC('day', archived_at) AS archived_at__day
  FROM ***************************.dim_users users_ds_source_src_28000
) subq_6
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_7
ON
  subq_6.archived_at__day = subq_7.ds
GROUP BY
  subq_7.alien_day
  , subq_6.archived_at__hour
