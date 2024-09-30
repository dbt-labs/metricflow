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
    subq_16.ds AS metric_time__day
    , COUNT(DISTINCT subq_14.bookers) AS every_2_days_bookers_2_days_ago
  FROM ***************************.mf_time_spine subq_16
  INNER JOIN (
    -- Join Self Over Time Range
    SELECT
      subq_13.ds AS metric_time__day
      , bookings_source_src_28000.guest_id AS bookers
    FROM ***************************.mf_time_spine subq_13
    INNER JOIN
      ***************************.fct_bookings bookings_source_src_28000
    ON
      (
        DATE_TRUNC('day', bookings_source_src_28000.ds) <= subq_13.ds
      ) AND (
        DATE_TRUNC('day', bookings_source_src_28000.ds) > subq_13.ds - INTERVAL 2 day
      )
  ) subq_14
  ON
    subq_16.ds - INTERVAL 2 day = subq_14.metric_time__day
  GROUP BY
    subq_16.ds
) subq_19
