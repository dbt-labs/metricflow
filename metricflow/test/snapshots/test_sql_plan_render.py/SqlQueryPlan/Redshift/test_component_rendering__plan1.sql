-- test1
SELECT
  SUM(1) AS bookings
  , b.country AS user__country
  , c.country AS listing__country
FROM demo.fct_bookings a
