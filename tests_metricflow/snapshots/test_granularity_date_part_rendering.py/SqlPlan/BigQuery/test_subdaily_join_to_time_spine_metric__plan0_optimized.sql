test_name: test_subdaily_join_to_time_spine_metric
test_filename: test_granularity_date_part_rendering.py
sql_engine: BigQuery
---
-- Join to Time Spine Dataset
-- Compute Metrics via Expressions
SELECT
  time_spine_src_28005.ts AS metric_time__hour
  , nr_subq_9.archived_users AS subdaily_join_to_time_spine_metric
FROM ***************************.mf_time_spine_hour time_spine_src_28005
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
      DATETIME_TRUNC(archived_at, hour) AS metric_time__hour
      , 1 AS archived_users
    FROM ***************************.dim_users users_ds_source_src_28000
  ) nr_subq_8
  GROUP BY
    metric_time__hour
) nr_subq_9
ON
  time_spine_src_28005.ts = nr_subq_9.metric_time__hour
