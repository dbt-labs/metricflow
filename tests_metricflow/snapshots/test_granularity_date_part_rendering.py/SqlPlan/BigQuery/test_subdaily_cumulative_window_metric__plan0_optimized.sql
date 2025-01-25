test_name: test_subdaily_cumulative_window_metric
test_filename: test_granularity_date_part_rendering.py
sql_engine: BigQuery
---
-- Join Self Over Time Range
-- Pass Only Elements: ['archived_users', 'metric_time__hour']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  nr_subq_8.ts AS metric_time__hour
  , SUM(nr_subq_6.archived_users) AS subdaily_cumulative_window_metric
FROM ***************************.mf_time_spine_hour nr_subq_8
INNER JOIN (
  -- Read Elements From Semantic Model 'users_ds_source'
  -- Metric Time Dimension 'archived_at'
  SELECT
    DATETIME_TRUNC(archived_at, hour) AS metric_time__hour
    , 1 AS archived_users
  FROM ***************************.dim_users users_ds_source_src_28000
) nr_subq_6
ON
  (
    nr_subq_6.metric_time__hour <= nr_subq_8.ts
  ) AND (
    nr_subq_6.metric_time__hour > DATE_SUB(CAST(nr_subq_8.ts AS DATETIME), INTERVAL 3 hour)
  )
GROUP BY
  metric_time__hour
