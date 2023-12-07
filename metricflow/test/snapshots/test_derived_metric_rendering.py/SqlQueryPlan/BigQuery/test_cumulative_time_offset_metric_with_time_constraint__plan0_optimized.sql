-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , every_2_days_bookers_2_days_ago AS every_2_days_bookers_2_days_ago
FROM (
  -- Join to Time Spine Dataset
  -- Pass Only Elements:
  --   ['bookers', 'metric_time__day']
  -- Constrain Time Range to [2019-12-19T00:00:00, 2020-01-02T00:00:00]
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_17.metric_time__day AS metric_time__day
    , COUNT(DISTINCT subq_16.bookers) AS every_2_days_bookers_2_days_ago
  FROM (
    -- Date Spine
    SELECT
      ds AS metric_time__day
    FROM ***************************.mf_time_spine subq_18
    WHERE ds BETWEEN '2019-12-19' AND '2020-01-02'
  ) subq_17
  INNER JOIN (
    -- Join Self Over Time Range
    SELECT
      subq_15.ds AS metric_time__day
      , bookings_source_src_10001.guest_id AS bookers
    FROM ***************************.mf_time_spine subq_15
    INNER JOIN
      ***************************.fct_bookings bookings_source_src_10001
    ON
      (
        DATE_TRUNC(bookings_source_src_10001.ds, day) <= subq_15.ds
      ) AND (
        DATE_TRUNC(bookings_source_src_10001.ds, day) > DATE_SUB(CAST(subq_15.ds AS DATETIME), INTERVAL 2 day)
      )
  ) subq_16
  ON
    DATE_SUB(CAST(subq_17.metric_time__day AS DATETIME), INTERVAL 2 day) = subq_16.metric_time__day
  WHERE subq_17.metric_time__day BETWEEN '2019-12-19' AND '2020-01-02'
  GROUP BY
    metric_time__day
) subq_23
