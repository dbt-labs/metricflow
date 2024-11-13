test_name: test_cumulative_time_offset_metric_with_time_constraint
test_filename: test_derived_metric_rendering.py
sql_engine: BigQuery
---
-- Read From CTE For node_id=cm_5
WITH cm_4_cte AS (
  -- Join to Time Spine Dataset
  -- Constrain Time Range to [2019-12-19T00:00:00, 2020-01-02T00:00:00]
  -- Pass Only Elements: ['bookers', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_17.metric_time__day AS metric_time__day
    , COUNT(DISTINCT subq_16.bookers) AS every_2_days_bookers_2_days_ago
  FROM (
    -- Time Spine
    SELECT
      ds AS metric_time__day
    FROM ***************************.mf_time_spine subq_18
    WHERE ds BETWEEN '2019-12-19' AND '2020-01-02'
  ) subq_17
  INNER JOIN (
    -- Join Self Over Time Range
    SELECT
      subq_15.ds AS metric_time__day
      , bookings_source_src_28000.guest_id AS bookers
    FROM ***************************.mf_time_spine subq_15
    INNER JOIN
      ***************************.fct_bookings bookings_source_src_28000
    ON
      (
        DATETIME_TRUNC(bookings_source_src_28000.ds, day) <= subq_15.ds
      ) AND (
        DATETIME_TRUNC(bookings_source_src_28000.ds, day) > DATE_SUB(CAST(subq_15.ds AS DATETIME), INTERVAL 2 day)
      )
  ) subq_16
  ON
    DATE_SUB(CAST(subq_17.metric_time__day AS DATETIME), INTERVAL 2 day) = subq_16.metric_time__day
  WHERE subq_17.metric_time__day BETWEEN '2019-12-19' AND '2020-01-02'
  GROUP BY
    metric_time__day
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
  ) subq_23
)

SELECT
  metric_time__day AS metric_time__day
  , every_2_days_bookers_2_days_ago AS every_2_days_bookers_2_days_ago
FROM cm_5_cte cm_5_cte
