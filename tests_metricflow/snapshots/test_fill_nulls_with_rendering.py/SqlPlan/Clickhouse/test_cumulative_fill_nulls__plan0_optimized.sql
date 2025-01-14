test_name: test_cumulative_fill_nulls
test_filename: test_fill_nulls_with_rendering.py
sql_engine: Clickhouse
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , COALESCE(bookers, 0) AS every_two_days_bookers_fill_nulls_with_0
FROM (
  -- Join to Time Spine Dataset
  SELECT
    time_spine_src_28006.ds AS metric_time__day
    , subq_17.bookers AS bookers
  FROM ***************************.mf_time_spine time_spine_src_28006
  LEFT OUTER JOIN (
    -- Join Self Over Time Range
    -- Pass Only Elements: ['bookers', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      subq_14.ds AS metric_time__day
      , COUNT(DISTINCT bookings_source_src_28000.guest_id) AS bookers
    FROM ***************************.mf_time_spine subq_14
    INNER JOIN
      ***************************.fct_bookings bookings_source_src_28000
    ON
      (
        date_trunc('day', bookings_source_src_28000.ds) <= subq_14.ds
      ) AND (
        date_trunc('day', bookings_source_src_28000.ds) > DATEADD(day, -2, subq_14.ds)
      )
    GROUP BY
      metric_time__day
  ) subq_17
  ON
    time_spine_src_28006.ds = subq_17.metric_time__day
) subq_21
