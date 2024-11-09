test_name: test_subdaily_join_to_time_spine_metric
test_filename: test_granularity_date_part_rendering.py
sql_engine: Postgres
---
-- Join to Time Spine Dataset
-- Compute Metrics via Expressions
SELECT
  subq_12.ts AS metric_time__hour
  , subq_10.archived_users AS subdaily_join_to_time_spine_metric
FROM ***************************.mf_time_spine_hour subq_12
LEFT OUTER JOIN (
  -- Aggregate Measures
  SELECT
    metric_time__hour
    , SUM(archived_users) AS archived_users
  FROM (
    -- Read Elements From Semantic Model 'users_ds_source'
    -- Metric Time Dimension 'archived_at'
    -- Pass Only Elements: ['archived_users', 'metric_time__hour']
    SELECT
      DATE_TRUNC('hour', archived_at) AS metric_time__hour
      , 1 AS archived_users
    FROM ***************************.dim_users users_ds_source_src_28000
  ) subq_9
  GROUP BY
    metric_time__hour
) subq_10
ON
  subq_12.ts = subq_10.metric_time__hour
