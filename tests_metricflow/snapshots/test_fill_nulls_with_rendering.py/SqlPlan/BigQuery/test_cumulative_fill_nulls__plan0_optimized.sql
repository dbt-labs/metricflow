test_name: test_cumulative_fill_nulls
test_filename: test_fill_nulls_with_rendering.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , COALESCE(bookers, 0) AS every_two_days_bookers_fill_nulls_with_0
FROM (
  -- Join to Time Spine Dataset
  SELECT
    time_spine_src_28006.ds AS metric_time__day
    , subq_18.bookers AS bookers
  FROM ***************************.mf_time_spine time_spine_src_28006
  LEFT OUTER JOIN (
    -- Join Self Over Time Range
    -- Pass Only Elements: ['bookers', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      subq_15.ds AS metric_time__day
      , COUNT(DISTINCT bookings_source_src_28000.guest_id) AS bookers
    FROM ***************************.mf_time_spine subq_15
    INNER JOIN
      ***************************.fct_bookings bookings_source_src_28000
    ON
      (
        TIMESTAMP_TRUNC(bookings_source_src_28000.ds, day) <= subq_15.ds
      ) AND (
        TIMESTAMP_TRUNC(bookings_source_src_28000.ds, day) > DATE_SUB(CAST(subq_15.ds AS DATETIME), INTERVAL 2 day)
      )
    GROUP BY
      metric_time__day
  ) subq_18
  ON
    time_spine_src_28006.ds = subq_18.metric_time__day
) subq_22
