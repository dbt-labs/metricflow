-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  metric_time
  , SUM(bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements:
  --   ['bookings', 'metric_time']
  SELECT
    ds AS metric_time
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_1
) subq_2
GROUP BY
  metric_time
