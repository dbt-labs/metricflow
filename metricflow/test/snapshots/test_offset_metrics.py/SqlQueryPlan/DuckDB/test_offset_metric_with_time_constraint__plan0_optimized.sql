-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , bookings_2_weeks_ago AS bookings_2_weeks_ago
FROM (
  -- Join to Time Spine Dataset
  -- Pass Only Elements:
  --   ['bookings', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_12.metric_time__day AS metric_time__day
    , SUM(subq_11.bookings) AS bookings_2_weeks_ago
  FROM (
    -- Date Spine
    SELECT
      ds AS metric_time__day
    FROM ***************************.mf_time_spine subq_13
    WHERE ds BETWEEN '2020-01-01' AND '2020-01-01'
  ) subq_12
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-01T00:00:00]
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_10001
    WHERE DATE_TRUNC('day', ds) BETWEEN '2020-01-01' AND '2020-01-01'
  ) subq_11
  ON
    subq_12.metric_time__day - INTERVAL 14 day = subq_11.metric_time__day
  GROUP BY
    subq_12.metric_time__day
) subq_17
