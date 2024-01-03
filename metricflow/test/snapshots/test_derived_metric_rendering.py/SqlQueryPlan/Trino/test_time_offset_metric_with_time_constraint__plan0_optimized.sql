-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , bookings_5_days_ago AS bookings_5_day_lag
FROM (
  -- Join to Time Spine Dataset
  -- Pass Only Elements:
  --   ['bookings', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_10.metric_time__day AS metric_time__day
    , SUM(subq_9.bookings) AS bookings_5_days_ago
  FROM (
    -- Date Spine
    SELECT
      ds AS metric_time__day
    FROM ***************************.mf_time_spine subq_11
    WHERE ds BETWEEN timestamp '2019-12-19' AND timestamp '2020-01-02'
  ) subq_10
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_10001
  ) subq_9
  ON
    DATE_ADD('day', -5, subq_10.metric_time__day) = subq_9.metric_time__day
  GROUP BY
    subq_10.metric_time__day
) subq_15
