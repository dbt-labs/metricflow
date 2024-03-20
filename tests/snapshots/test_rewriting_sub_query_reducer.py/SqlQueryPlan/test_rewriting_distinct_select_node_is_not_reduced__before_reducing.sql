-- test0
SELECT
  a.booking_value
FROM (
  -- test1
  SELECT DISTINCT
    a.booking_value
    , a.bookings
  FROM demo.fct_bookings a
) b
