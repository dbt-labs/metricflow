test_name: test_derived_offset_cumulative_metric
test_filename: test_derived_metric_rendering.py
sql_engine: DuckDB
---
-- Read From CTE For node_id=cm_5
WITH cm_4_cte AS (
  -- Join to Time Spine Dataset
  -- Pass Only Elements: ['bookers', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_17.ds AS metric_time__day
    , COUNT(DISTINCT subq_15.bookers) AS every_2_days_bookers_2_days_ago
  FROM ***************************.mf_time_spine subq_17
  INNER JOIN (
    -- Join Self Over Time Range
    SELECT
      subq_14.ds AS metric_time__day
      , bookings_source_src_28000.guest_id AS bookers
    FROM ***************************.mf_time_spine subq_14
    INNER JOIN
      ***************************.fct_bookings bookings_source_src_28000
    ON
      (
        DATE_TRUNC('day', bookings_source_src_28000.ds) <= subq_14.ds
      ) AND (
        DATE_TRUNC('day', bookings_source_src_28000.ds) > subq_14.ds - INTERVAL 2 day
      )
  ) subq_15
  ON
    subq_17.ds - INTERVAL 2 day = subq_15.metric_time__day
  GROUP BY
    subq_17.ds
)

, cm_5_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , every_2_days_bookers_2_days_ago AS every_2_days_bookers_2_days_ago
  FROM (
    -- Read From CTE For node_id=cm_4
    SELECT
      metric_time__day
      , every_2_days_bookers_2_days_ago
    FROM cm_4_cte cm_4_cte
  ) subq_21
)

SELECT
  metric_time__day AS metric_time__day
  , every_2_days_bookers_2_days_ago AS every_2_days_bookers_2_days_ago
FROM cm_5_cte cm_5_cte
