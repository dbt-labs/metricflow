test_name: test_subdaily_granularity_overrides_metric_default_granularity
test_filename: test_granularity_date_part_rendering.py
sql_engine: DuckDB
---
-- Join to Time Spine Dataset
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  time_spine_src_28005.ts AS metric_time__hour
  , subq_12.subdaily_join_to_time_spine_metric AS subdaily_join_to_time_spine_metric
FROM ***************************.mf_time_spine_hour time_spine_src_28005
LEFT OUTER JOIN (
  -- Aggregate Inputs for Simple Metrics
  SELECT
    metric_time__hour
    , SUM(subdaily_join_to_time_spine_metric) AS subdaily_join_to_time_spine_metric
  FROM (
    -- Read Elements From Semantic Model 'users_ds_source'
    -- Metric Time Dimension 'archived_at'
    -- Pass Only Elements: ['subdaily_join_to_time_spine_metric', 'metric_time__hour']
    SELECT
      DATE_TRUNC('hour', archived_at) AS metric_time__hour
      , 1 AS subdaily_join_to_time_spine_metric
    FROM ***************************.dim_users users_ds_source_src_28000
  ) subq_11
  GROUP BY
    metric_time__hour
) subq_12
ON
  time_spine_src_28005.ts = subq_12.metric_time__hour
