-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , every_2_days_bookers_2_days_ago AS every_2_days_bookers_2_days_ago
FROM (
  -- Join to Time Spine Dataset
  -- Pass Only Elements:
  --   ['bookers', 'metric_time__day']
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
      , bookings_source_src_10001.guest_id AS bookers
    FROM ***************************.mf_time_spine subq_14
    INNER JOIN
      ***************************.fct_bookings bookings_source_src_10001
    ON
      (
        DATE_TRUNC('day', bookings_source_src_10001.ds) <= subq_14.ds
      ) AND (
        DATE_TRUNC('day', bookings_source_src_10001.ds) > subq_14.ds - MAKE_INTERVAL(days => 2)
      )
  ) subq_15
  ON
    subq_17.ds - MAKE_INTERVAL(days => 2) = subq_15.metric_time__day
  GROUP BY
    subq_17.ds
) subq_21
