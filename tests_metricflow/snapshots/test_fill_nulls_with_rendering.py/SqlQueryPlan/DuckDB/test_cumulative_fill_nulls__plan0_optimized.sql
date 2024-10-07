-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , COALESCE(bookers, 0) AS every_two_days_bookers_fill_nulls_with_0
FROM (
  -- Join to Time Spine Dataset
  SELECT
    subq_16.ds AS metric_time__day
    , subq_14.bookers AS bookers
  FROM ***************************.mf_time_spine subq_16
  LEFT OUTER JOIN (
    -- Join Self Over Time Range
    -- Pass Only Elements: ['bookers', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      subq_12.ds AS metric_time__day
      , COUNT(DISTINCT bookings_source_src_28000.guest_id) AS bookers
    FROM ***************************.mf_time_spine subq_12
    INNER JOIN
      ***************************.fct_bookings bookings_source_src_28000
    ON
      (
        DATE_TRUNC('day', bookings_source_src_28000.ds) <= subq_12.ds
      ) AND (
        DATE_TRUNC('day', bookings_source_src_28000.ds) > subq_12.ds - INTERVAL 2 day
      )
    GROUP BY
      subq_12.ds
  ) subq_14
  ON
    subq_16.ds = subq_14.metric_time__day
) subq_17
