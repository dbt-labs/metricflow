-- Constrain Output with WHERE
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , COUNT(DISTINCT bookers) AS every_two_days_bookers
FROM (
  -- Join Self Over Time Range
  -- Pass Only Elements: ['bookers', 'metric_time__day']
  SELECT
    subq_10.ds AS metric_time__day
    , bookings_source_src_28000.guest_id AS bookers
  FROM ***************************.mf_time_spine subq_10
  INNER JOIN
    ***************************.fct_bookings bookings_source_src_28000
  ON
    (
      DATE_TRUNC('day', bookings_source_src_28000.ds) <= subq_10.ds
    ) AND (
      DATE_TRUNC('day', bookings_source_src_28000.ds) > subq_10.ds - INTERVAL 2 day
    )
) subq_11
WHERE metric_time__day = '2020-01-03' or metric_time__day = '2020-01-07'
GROUP BY
  metric_time__day
