-- Aggregate Measures
SELECT
  SUM(subq_0.bookings) AS bookings
  , SUM(subq_0.instant_bookings) AS instant_bookings
  , COUNT(DISTINCT subq_0.bookers) AS bookers
  , AVG(subq_0.average_booking_value) AS average_booking_value
FROM (
  -- Read From SemanticModelDataSet('bookings_source')
  -- Pass Only Elements: ['bookings', 'instant_bookings', 'average_booking_value', 'bookers']
  SELECT
    1 AS bookings
    , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
    , bookings_source_src_28000.guest_id AS bookers
    , bookings_source_src_28000.booking_value AS average_booking_value
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_0
