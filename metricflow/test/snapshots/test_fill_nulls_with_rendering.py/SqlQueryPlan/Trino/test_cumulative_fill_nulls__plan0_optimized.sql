-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , COALESCE(bookers, 0) AS every_two_days_bookers_fill_nulls_with_0
FROM (
  -- Join to Time Spine Dataset
  SELECT
    subq_18.ds AS metric_time__day
    , subq_16.bookers AS bookers
  FROM ***************************.mf_time_spine subq_18
  LEFT OUTER JOIN (
    -- Join Self Over Time Range
    -- Pass Only Elements:
    --   ['bookers', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      subq_13.ds AS metric_time__day
      , COUNT(DISTINCT bookings_source_src_10001.guest_id) AS bookers
    FROM ***************************.mf_time_spine subq_13
    INNER JOIN
      ***************************.fct_bookings bookings_source_src_10001
    ON
      (
        DATE_TRUNC('day', bookings_source_src_10001.ds) <= subq_13.ds
      ) AND (
        DATE_TRUNC('day', bookings_source_src_10001.ds) > DATE_ADD('day', -2, subq_13.ds)
      )
    GROUP BY
      subq_13.ds
  ) subq_16
  ON
    subq_18.ds = subq_16.metric_time__day
) subq_19
