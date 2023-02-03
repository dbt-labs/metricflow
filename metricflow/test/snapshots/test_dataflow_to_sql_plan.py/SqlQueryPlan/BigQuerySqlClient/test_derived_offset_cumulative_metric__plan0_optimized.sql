-- Compute Metrics via Expressions
SELECT
  metric_time
  , every_2_days_bookers_2_days_ago AS every_2_days_bookers_2_days_ago
FROM (
  -- Join to Time Spine Dataset
  -- Pass Only Elements:
  --   ['bookers', 'metric_time']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    DATE_TRUNC(subq_16.metric_time, day) AS metric_time
    , COUNT(DISTINCT subq_15.bookers) AS every_2_days_bookers_2_days_ago
  FROM (
    -- Date Spine
    SELECT
      ds AS metric_time
    FROM ***************************.mf_time_spine subq_17
    GROUP BY
      metric_time
  ) subq_16
  INNER JOIN (
    -- Join Self Over Time Range
    SELECT
      subq_13.metric_time AS metric_time
      , bookings_source_src_10001.guest_id AS bookers
    FROM (
      -- Date Spine
      SELECT
        ds AS metric_time
      FROM ***************************.mf_time_spine subq_14
      GROUP BY
        metric_time
    ) subq_13
    INNER JOIN (
      -- User Defined SQL Query
      SELECT * FROM ***************************.fct_bookings
    ) bookings_source_src_10001
    ON
      (
        bookings_source_src_10001.ds <= subq_13.metric_time
      ) AND (
        bookings_source_src_10001.ds > DATE_SUB(CAST(subq_13.metric_time AS DATETIME), INTERVAL 2 day)
      )
  ) subq_15
  ON
    DATE_SUB(CAST(subq_16.metric_time AS DATETIME), INTERVAL 2 day) = subq_15.metric_time
  GROUP BY
    metric_time
) subq_21
