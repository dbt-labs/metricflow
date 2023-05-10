-- Constrain Output with WHERE
-- Pass Only Elements:
--   ['booking_value', 'metric_time']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  metric_time
  , SUM(booking_value) AS instant_booking_value
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements:
  --   ['booking_value', 'is_instant', 'metric_time']
  SELECT
    ds AS metric_time
    , is_instant
    , booking_value
  FROM ***************************.fct_bookings bookings_source_src_10001
) subq_8
WHERE is_instant
GROUP BY
  metric_time
