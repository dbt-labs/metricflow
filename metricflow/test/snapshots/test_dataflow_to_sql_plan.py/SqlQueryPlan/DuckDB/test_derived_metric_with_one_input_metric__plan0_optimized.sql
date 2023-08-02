-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , bookings_5_days_ago AS bookings_5_day_lag
FROM (
  -- Join to Time Spine Dataset
  SELECT
    subq_14.ds AS metric_time__day
    , subq_12.bookings_5_days_ago AS bookings_5_days_ago
  FROM ***************************.mf_time_spine subq_14
  INNER JOIN (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , SUM(bookings) AS bookings_5_days_ago
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements:
      --   ['bookings', 'metric_time__day']
      SELECT
        ds AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_10001
    ) subq_10
    GROUP BY
      metric_time__day
  ) subq_12
  ON
    subq_14.ds - INTERVAL 5 day = subq_12.metric_time__day
) subq_15
