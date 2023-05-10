-- Read Elements From Data Source 'bookings_source'
-- Metric Time Dimension 'ds'
-- Pass Only Elements:
--   ['booking_value', 'metric_time']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  ds AS metric_time
  , SUM(booking_value) AS instant_booking_value
FROM ***************************.fct_bookings bookings_source_src_10001
GROUP BY
  ds
