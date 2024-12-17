test_name: test_derived_offset_cumulative_metric
test_filename: test_derived_metric_rendering.py
sql_engine: Redshift
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
        DATE_TRUNC('day', bookings_source_src_28000.ds) <= subq_15.ds
      ) AND (
        DATE_TRUNC('day', bookings_source_src_28000.ds) > DATEADD(day, -2, subq_15.ds)
      )
  ) subq_16
  ON
    DATEADD(day, -2, time_spine_src_28006.ds) = subq_16.metric_time__day
  GROUP BY
    time_spine_src_28006.ds
) subq_23
