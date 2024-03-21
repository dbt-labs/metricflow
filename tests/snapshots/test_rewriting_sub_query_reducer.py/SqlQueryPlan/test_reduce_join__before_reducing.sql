-- query
SELECT
  SUM(bookings_src.bookings) AS bookings
  , listings_src.country_latest AS listing__country_latest
  , bookings_src.ds AS ds
FROM (
  -- bookings_src
  SELECT
    fct_bookings_src.booking AS bookings
    , 1 AS ds
    , fct_bookings_src.listing_id AS listing
  FROM demo.fct_bookings fct_bookings_src
) bookings_src
LEFT OUTER JOIN (
  -- listings_src
  SELECT
    dim_listings_src.country AS country_latest
    , dim_listings_src.listing_id AS listing
  FROM demo.dim_listings dim_listings_src
) listings_src
ON
  bookings_src.listing = listings_src.listing
WHERE bookings_src.ds <= '2020-01-05'
GROUP BY
  bookings_src.ds
