-- Read Elements From Semantic Model 'bookings_source'
-- Pass Only Elements:
--   ['bookings', 'instant_bookings', 'average_booking_value', 'bookers']
-- Aggregate Measures
SELECT
  SUM(1) AS bookings
  , SUM(CASE WHEN is_instant THEN 1 ELSE 0 END) AS instant_bookings
  , COUNT(DISTINCT guest_id) AS bookers
  , AVG(booking_value) AS average_booking_value
FROM ***************************.fct_bookings bookings_source_src_10001
