-- test5
SELECT
  SUM(1) AS bookings
  , b.country AS user__country
  , c.country AS listing__country
FROM demo.fct_bookings a
LEFT OUTER JOIN
  demo.dim_users b
ON
  a.user_id = b.user_id
LEFT OUTER JOIN
  demo.dim_listings c
ON
  a.user_id = c.user_id
GROUP BY
  b.country
  , c.country
