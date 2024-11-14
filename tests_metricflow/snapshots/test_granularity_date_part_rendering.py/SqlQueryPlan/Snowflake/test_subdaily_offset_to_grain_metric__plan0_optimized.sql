test_name: test_subdaily_offset_to_grain_metric
test_filename: test_granularity_date_part_rendering.py
sql_engine: Snowflake
---
-- Read From CTE For node_id=cm_5
WITH cm_4_cte AS (
  -- Join to Time Spine Dataset
  -- Pass Only Elements: ['archived_users', 'metric_time__hour']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_11.ts AS metric_time__hour
    , SUM(subq_9.archived_users) AS archived_users
  FROM ***************************.mf_time_spine_hour subq_11
  INNER JOIN (
    -- Read Elements From Semantic Model 'users_ds_source'
    -- Metric Time Dimension 'archived_at'
    SELECT
      DATE_TRUNC('hour', archived_at) AS metric_time__hour
      , 1 AS archived_users
    FROM ***************************.dim_users users_ds_source_src_28000
  ) subq_9
  ON
    DATE_TRUNC('hour', subq_11.ts) = subq_9.metric_time__hour
  GROUP BY
    subq_11.ts
)

, cm_5_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__hour
    , archived_users AS subdaily_offset_grain_to_date_metric
  FROM (
    -- Read From CTE For node_id=cm_4
    SELECT
      metric_time__hour
      , archived_users
    FROM cm_4_cte cm_4_cte
  ) subq_15
)

SELECT
  metric_time__hour AS metric_time__hour
  , subdaily_offset_grain_to_date_metric AS subdaily_offset_grain_to_date_metric
FROM cm_5_cte cm_5_cte
