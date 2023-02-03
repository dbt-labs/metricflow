-- Compute Metrics via Expressions
SELECT
  metric_time
  , bookings_5_days_ago AS bookings_5_day_lag
FROM (
  -- Join to Time Spine Dataset
  -- Pass Only Elements:
  --   ['bookings', 'metric_time']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    DATE_TRUNC(subq_10.metric_time, day) AS metric_time
    , SUM(subq_9.bookings) AS bookings_5_days_ago
  FROM (
    -- Date Spine
    SELECT
      ds AS metric_time
    FROM ***************************.mf_time_spine subq_11
    GROUP BY
      metric_time
  ) subq_10
  INNER JOIN (
    -- Read Elements From Data Source 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      ds AS metric_time
      , 1 AS bookings
    FROM (
      -- User Defined SQL Query
      SELECT * FROM ***************************.fct_bookings
    ) bookings_source_src_10001
  ) subq_9
  ON
    DATE_SUB(CAST(subq_10.metric_time AS DATETIME), INTERVAL 5 day) = subq_9.metric_time
  GROUP BY
    metric_time
) subq_15
