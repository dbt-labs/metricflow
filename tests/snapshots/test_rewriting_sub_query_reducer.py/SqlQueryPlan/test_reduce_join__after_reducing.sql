-- query
SELECT
  SUM(bookings_src.bookings) AS bookings
  , dim_listings_src.country AS listing__country_latest
  , bookings_src.ds AS ds
FROM (
  -- bookings_src
  SELECT
    fct_bookings_src.booking AS bookings
    , 1 AS ds
    , fct_bookings_src.listing_id AS listing
  FROM demo.fct_bookings fct_bookings_src
) bookings_src
LEFT OUTER JOIN
  demo.dim_listings dim_listings_src
ON
  bookings_src.listing = dim_listings_src.listing_id
WHERE bookings_src.ds <= '2020-01-05'
GROUP BY
  bookings_src.ds
