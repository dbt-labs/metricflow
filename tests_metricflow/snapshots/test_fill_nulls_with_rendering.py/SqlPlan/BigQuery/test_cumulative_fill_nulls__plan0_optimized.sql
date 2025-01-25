test_name: test_cumulative_fill_nulls
test_filename: test_fill_nulls_with_rendering.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , COALESCE(bookers, 0) AS every_two_days_bookers_fill_nulls_with_0
FROM (
  -- Join to Time Spine Dataset
  SELECT
    time_spine_src_28006.ds AS metric_time__day
    , nr_subq_15.bookers AS bookers
  FROM ***************************.mf_time_spine time_spine_src_28006
  LEFT OUTER JOIN (
    -- Join Self Over Time Range
    -- Pass Only Elements: ['bookers', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      nr_subq_12.ds AS metric_time__day
      , COUNT(DISTINCT bookings_source_src_28000.guest_id) AS bookers
    FROM ***************************.mf_time_spine nr_subq_12
    INNER JOIN
      ***************************.fct_bookings bookings_source_src_28000
    ON
      (
        DATETIME_TRUNC(bookings_source_src_28000.ds, day) <= nr_subq_12.ds
      ) AND (
        DATETIME_TRUNC(bookings_source_src_28000.ds, day) > DATE_SUB(CAST(nr_subq_12.ds AS DATETIME), INTERVAL 2 day)
      )
    GROUP BY
      metric_time__day
  ) nr_subq_15
  ON
    time_spine_src_28006.ds = nr_subq_15.metric_time__day
) nr_subq_19
