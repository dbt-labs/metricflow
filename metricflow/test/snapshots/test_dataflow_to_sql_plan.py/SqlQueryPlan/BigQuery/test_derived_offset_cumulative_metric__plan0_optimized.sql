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
        DATE_TRUNC(bookings_source_src_10001.ds, day) <= subq_14.ds
      ) AND (
        DATE_TRUNC(bookings_source_src_10001.ds, day) > DATE_SUB(CAST(subq_14.ds AS DATETIME), INTERVAL 2 day)
      )
  ) subq_15
  ON
    DATE_SUB(CAST(subq_17.ds AS DATETIME), INTERVAL 2 day) = subq_15.metric_time__day
  GROUP BY
    metric_time__day
) subq_21
