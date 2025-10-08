test_name: test_cumulative_fill_nulls
test_filename: test_fill_nulls_with_rendering.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , COALESCE(bookers_fill_nulls_with_0_join_to_timespine, 0) AS every_two_days_bookers_fill_nulls_with_0
FROM (
  -- Join to Time Spine Dataset
  SELECT
    time_spine_src_28006.ds AS metric_time__day
    , subq_19.bookers_fill_nulls_with_0_join_to_timespine AS bookers_fill_nulls_with_0_join_to_timespine
  FROM ***************************.mf_time_spine time_spine_src_28006
  LEFT OUTER JOIN (
    -- Join Self Over Time Range
    -- Pass Only Elements: ['bookers_fill_nulls_with_0_join_to_timespine', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      subq_16.ds AS metric_time__day
      , COUNT(DISTINCT bookings_source_src_28000.guest_id) AS bookers_fill_nulls_with_0_join_to_timespine
    FROM ***************************.mf_time_spine subq_16
    INNER JOIN
      ***************************.fct_bookings bookings_source_src_28000
    ON
      (
        DATE_TRUNC('day', bookings_source_src_28000.ds) <= subq_16.ds
      ) AND (
        DATE_TRUNC('day', bookings_source_src_28000.ds) > subq_16.ds - INTERVAL 2 day
      )
    GROUP BY
      subq_16.ds
  ) subq_19
  ON
    time_spine_src_28006.ds = subq_19.metric_time__day
) subq_23
