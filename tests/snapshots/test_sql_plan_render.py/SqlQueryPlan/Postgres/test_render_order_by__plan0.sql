-- test0
SELECT
  a.booking_value
  , a.bookings
FROM demo.fct_bookings a
ORDER BY a.booking_value, a.bookings DESC
