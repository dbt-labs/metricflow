test_name: test_cumulative_time_offset_metric_with_time_constraint
test_filename: test_derived_metric_rendering.py
sql_engine: Databricks
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , every_2_days_bookers_2_days_ago AS every_2_days_bookers_2_days_ago
FROM (
  -- Join to Time Spine Dataset
  -- Constrain Time Range to [2019-12-19T00:00:00, 2020-01-02T00:00:00]
  -- Pass Only Elements: ['bookers', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_18.ds AS metric_time__day
    , COUNT(DISTINCT subq_16.bookers) AS every_2_days_bookers_2_days_ago
  FROM ***************************.mf_time_spine subq_18
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
        DATE_TRUNC('day', bookings_source_src_28000.ds) <= subq_15.ds
      ) AND (
        DATE_TRUNC('day', bookings_source_src_28000.ds) > DATEADD(day, -2, subq_15.ds)
      )
  ) subq_16
  ON
    DATEADD(day, -2, subq_18.ds) = subq_16.metric_time__day
  WHERE subq_18.ds BETWEEN '2019-12-19' AND '2020-01-02'
  GROUP BY
    subq_18.ds
) subq_23
