test_name: test_subdaily_offset_to_grain_metric
test_filename: test_granularity_date_part_rendering.py
sql_engine: Postgres
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__hour
  , archived_users AS subdaily_offset_grain_to_date_metric
FROM (
  -- Join to Time Spine Dataset
  -- Compute Metrics via Expressions
  SELECT
    time_spine_src_28005.ts AS metric_time__hour
    , subq_16.__archived_users AS archived_users
  FROM ***************************.mf_time_spine_hour time_spine_src_28005
  INNER JOIN (
    -- Aggregate Inputs for Simple Metrics
    SELECT
      metric_time__hour
      , SUM(__archived_users) AS __archived_users
    FROM (
      -- Read Elements From Semantic Model 'users_ds_source'
      -- Metric Time Dimension 'archived_at'
      -- Pass Only Elements: ['__archived_users', 'metric_time__hour']
      -- Pass Only Elements: ['__archived_users', 'metric_time__hour']
      SELECT
        DATE_TRUNC('hour', archived_at) AS metric_time__hour
        , 1 AS __archived_users
      FROM ***************************.dim_users users_ds_source_src_28000
    ) subq_15
    GROUP BY
      metric_time__hour
  ) subq_16
  ON
    DATE_TRUNC('hour', time_spine_src_28005.ts) = subq_16.metric_time__hour
) subq_22
