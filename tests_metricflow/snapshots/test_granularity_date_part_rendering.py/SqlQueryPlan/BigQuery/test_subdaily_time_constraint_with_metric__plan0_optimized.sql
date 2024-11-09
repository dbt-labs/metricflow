test_name: test_subdaily_time_constraint_with_metric
test_filename: test_granularity_date_part_rendering.py
sql_engine: BigQuery
---
-- Join to Time Spine Dataset
-- Constrain Time Range to [2020-01-01T02:00:00, 2020-01-01T05:00:00]
-- Compute Metrics via Expressions
SELECT
  subq_14.metric_time__hour AS metric_time__hour
  , subq_13.archived_users AS subdaily_join_to_time_spine_metric
FROM (
  -- Time Spine
  SELECT
    ts AS metric_time__hour
  FROM ***************************.mf_time_spine_hour subq_15
  WHERE ts BETWEEN '2020-01-01 02:00:00' AND '2020-01-01 05:00:00'
) subq_14
LEFT OUTER JOIN (
  -- Aggregate Measures
  SELECT
    metric_time__hour
    , SUM(archived_users) AS archived_users
  FROM (
    -- Read Elements From Semantic Model 'users_ds_source'
    -- Metric Time Dimension 'archived_at'
    -- Constrain Time Range to [2020-01-01T02:00:00, 2020-01-01T05:00:00]
    -- Pass Only Elements: ['archived_users', 'metric_time__hour']
    SELECT
      DATETIME_TRUNC(archived_at, hour) AS metric_time__hour
      , 1 AS archived_users
    FROM ***************************.dim_users users_ds_source_src_28000
    WHERE DATETIME_TRUNC(archived_at, hour) BETWEEN '2020-01-01 02:00:00' AND '2020-01-01 05:00:00'
  ) subq_12
  GROUP BY
    metric_time__hour
) subq_13
ON
  subq_14.metric_time__hour = subq_13.metric_time__hour
WHERE subq_14.metric_time__hour BETWEEN '2020-01-01 02:00:00' AND '2020-01-01 05:00:00'
