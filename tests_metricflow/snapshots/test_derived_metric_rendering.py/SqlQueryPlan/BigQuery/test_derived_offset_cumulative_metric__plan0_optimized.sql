test_name: test_derived_offset_cumulative_metric
test_filename: test_derived_metric_rendering.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , every_2_days_bookers_2_days_ago AS every_2_days_bookers_2_days_ago
FROM (
  -- Join to Time Spine Dataset
  -- Pass Only Elements: ['bookers', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    time_spine_src_28006.ds AS metric_time__day
    , COUNT(DISTINCT subq_16.bookers) AS every_2_days_bookers_2_days_ago
  FROM ***************************.mf_time_spine time_spine_src_28006
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
    DATE_SUB(CAST(time_spine_src_28006.ds AS DATETIME), INTERVAL 2 day) = subq_16.metric_time__day
  GROUP BY
    metric_time__day
) subq_23
