test_name: test_subdaily_time_constraint_with_metric
test_filename: test_granularity_date_part_rendering.py
sql_engine: Snowflake
---
-- Join to Time Spine Dataset
-- Constrain Time Range to [2020-01-01T02:00:00, 2020-01-01T05:00:00]
-- Compute Metrics via Expressions
SELECT
  subq_19.metric_time__hour AS metric_time__hour
  , subq_15.archived_users AS subdaily_join_to_time_spine_metric
FROM (
  -- Read From Time Spine 'mf_time_spine_hour'
  -- Change Column Aliases
  -- Constrain Time Range to [2020-01-01T02:00:00, 2020-01-01T05:00:00]
  -- Pass Only Elements: ['metric_time__hour',]
  SELECT
    ts AS metric_time__hour
  FROM ***************************.mf_time_spine_hour time_spine_src_28005
  WHERE ts BETWEEN '2020-01-01 02:00:00' AND '2020-01-01 05:00:00'
) subq_19
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
      DATE_TRUNC('hour', archived_at) AS metric_time__hour
      , 1 AS archived_users
    FROM ***************************.dim_users users_ds_source_src_28000
    WHERE DATE_TRUNC('hour', archived_at) BETWEEN '2020-01-01 02:00:00' AND '2020-01-01 05:00:00'
  ) subq_14
  GROUP BY
    metric_time__hour
) subq_15
ON
  subq_19.metric_time__hour = subq_15.metric_time__hour
WHERE subq_19.metric_time__hour BETWEEN '2020-01-01 02:00:00' AND '2020-01-01 05:00:00'
