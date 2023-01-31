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
    subq_11.ds AS metric_time
    , SUM(subq_9.bookings) AS bookings_5_days_ago
  FROM ***************************.mf_time_spine subq_11
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
    subq_11.ds - INTERVAL 5 day = subq_9.metric_time
  GROUP BY
    subq_11.ds
) subq_15
